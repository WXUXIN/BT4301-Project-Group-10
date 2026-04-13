#!/usr/bin/env python3
"""
Sync the current MLflow champion model into ./deploy_to_production/artifacts/current/.

Two operating modes
-------------------
Remote mode (default)
    Connects to the MLflow tracking server over HTTP.
    The server must be running.

    python deploy_to_production/sync_champion_from_mlflow.py

Local mode  (--local flag)
    Reads mlflow.db (SQLite) and copies model.pkl directly from the local
    mlruns/ directory.  The MLflow HTTP server does NOT need to be running.

    python deploy_to_production/sync_champion_from_mlflow.py --local

Local mode works because the project stores everything on disk:
  - mlflow.db holds the champion alias → version mapping, storage_location,
    and all model version tags (decision_threshold, trained_up_to_period …)
  - mlruns/1/models/m-<uuid>/artifacts/model.pkl holds the actual Pipeline

After running this script, restart the serving container:
    docker compose -f deploy_to_production/compose.yaml restart churn-api
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from dataops.airflow.mlops.mlops_config import (  # noqa: E402
    MLFLOW_TRACKING_URI,
    REGISTERED_MODEL_NAME,
    FEATURE_LIST_PATH,
    PROJECT_ROOT,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "artifacts" / "current"

_DEFAULT_DB_PATH = Path(PROJECT_ROOT) / "mlflow.db"


# ===========================================================================
# Shared helpers
# ===========================================================================

def _sync_feature_list(run_id: str | None = None) -> None:
    """
    Copy feature_lists.json from the mlops artifacts directory.
    Falls back to downloading selected_features.json from the MLflow run
    artifact store (HTTP mode only, run_id required).
    """
    dest = OUTPUT_DIR / "feature_lists.json"
    source = Path(FEATURE_LIST_PATH)

    if source.exists():
        shutil.copy2(str(source), str(dest))
        print(f"  Copied feature_lists.json from: {source}")
        return

    if run_id is None:
        print(
            f"  ERROR: feature_lists.json not found at {source} "
            "and no run_id available to fall back to.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Fallback: download selected_features.json from the MLflow run artifact
    import mlflow

    print(
        f"  feature_lists.json not found at {source}. "
        "Falling back to MLflow run artifact selected_features.json …"
    )
    try:
        with tempfile.TemporaryDirectory() as tmp:
            local_path = mlflow.artifacts.download_artifacts(
                artifact_uri=f"runs:/{run_id}/selected_features.json",
                dst_path=tmp,
            )
            with open(local_path) as f:
                data = json.load(f)

        features = data.get("selected_features", data)
        feature_dict = {"all": features} if isinstance(features, list) else features

        with open(dest, "w") as f:
            json.dump(feature_dict, f, indent=2)

        print(f"  feature_lists.json written ({len(feature_dict.get('all', []))} features)")

    except Exception as exc:
        print(f"  ERROR: Could not obtain feature_lists.json: {exc}", file=sys.stderr)
        sys.exit(1)


def _write_metadata(version: str, tags: dict) -> None:
    raw_threshold = tags.get("decision_threshold")
    raw_period = tags.get("trained_up_to_period")

    try:
        threshold = float(raw_threshold) if raw_threshold is not None else None
    except (ValueError, TypeError):
        threshold = None

    try:
        period = int(raw_period) if raw_period is not None and str(raw_period).isdigit() else raw_period
    except (ValueError, TypeError):
        period = raw_period

    metadata = {
        "registered_model_name": REGISTERED_MODEL_NAME,
        "champion_version": version,
        "decision_threshold": threshold,
        "trained_up_to_period": period,
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }

    dest = OUTPUT_DIR / "metadata.json"
    with open(dest, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"  metadata.json → {metadata}")


def _print_header(mode: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"Champion sync — Customer Churn Serving Layer  [{mode} mode]")
    print(f"{'=' * 60}")
    print(f"Registered model : {REGISTERED_MODEL_NAME}")
    print(f"Output directory : {OUTPUT_DIR}\n")


def _print_footer() -> None:
    print(f"\n{'=' * 60}")
    print("Sync complete.")
    print(f"Artifacts ready in: {OUTPUT_DIR}")
    print()
    print("Next steps:")
    print("  docker compose -f deploy_to_production/compose.yaml up -d --build   # first time")
    print("  docker compose -f deploy_to_production/compose.yaml restart churn-api  # after sync")
    print(f"{'=' * 60}\n")


# ===========================================================================
# LOCAL mode — reads SQLite + copies from mlruns/ directly
# ===========================================================================

def _local_sync(db_path: Path) -> None:
    """
    Sync without the MLflow HTTP server by querying mlflow.db directly
    and copying model.pkl from the local mlruns/ artifact store.
    """
    import sqlite3

    _print_header("local / offline")
    print(f"Database         : {db_path}\n")

    if not db_path.exists():
        print(f"ERROR: mlflow.db not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    # 1. Resolve champion alias → version number
    row = con.execute(
        "SELECT version FROM registered_model_aliases "
        "WHERE name = ? AND alias = 'champion'",
        (REGISTERED_MODEL_NAME,),
    ).fetchone()

    if row is None:
        print(
            f"ERROR: No 'champion' alias found for model '{REGISTERED_MODEL_NAME}' "
            f"in {db_path}.\n"
            "Make sure the retraining pipeline has promoted at least one champion.",
            file=sys.stderr,
        )
        sys.exit(1)

    version = str(row["version"])

    # 2. Get storage_location and run_id for that version
    mv_row = con.execute(
        "SELECT run_id, storage_location FROM model_versions "
        "WHERE name = ? AND version = ?",
        (REGISTERED_MODEL_NAME, version),
    ).fetchone()

    if mv_row is None:
        print(
            f"ERROR: Model version {version} not found in model_versions table.",
            file=sys.stderr,
        )
        sys.exit(1)

    run_id = mv_row["run_id"]
    storage_location = mv_row["storage_location"]  # file:///abs/path/to/artifacts

    # 3. Get tags (decision_threshold, trained_up_to_period …)
    tag_rows = con.execute(
        "SELECT key, value FROM model_version_tags "
        "WHERE name = ? AND version = ?",
        (REGISTERED_MODEL_NAME, version),
    ).fetchall()
    tags = {r["key"]: r["value"] for r in tag_rows}

    con.close()

    print(f"Champion found (via SQLite):")
    print(f"  version              : {version}")
    print(f"  run_id               : {run_id}")
    print(f"  trained_up_to_period : {tags.get('trained_up_to_period', 'n/a')}")
    print(f"  decision_threshold   : {tags.get('decision_threshold', 'n/a')}")
    print(f"  storage_location     : {storage_location}\n")

    # 4. Resolve file:// URI → local path
    parsed = urlparse(storage_location)
    artifacts_dir = Path(parsed.path)  # e.g. /abs/path/.../m-xxx/artifacts
    model_pkl_src = artifacts_dir / "model.pkl"

    if not model_pkl_src.exists():
        print(
            f"ERROR: model.pkl not found at {model_pkl_src}\n"
            f"  storage_location resolved to: {artifacts_dir}\n"
            f"  Files present: {list(artifacts_dir.iterdir()) if artifacts_dir.exists() else 'directory missing'}",
            file=sys.stderr,
        )
        sys.exit(1)

    # 5. Copy entire artifact directory → OUTPUT_DIR/model/
    model_dest = OUTPUT_DIR / "model"
    if model_dest.exists():
        shutil.rmtree(model_dest)
    shutil.copytree(str(artifacts_dir), str(model_dest))
    print(f"  Model copied: {model_pkl_src} → {model_dest}/")

    # 6. Sync feature list (local copy, no fallback needed since run_id is available)
    _sync_feature_list(run_id=None)   # prefer local file; no HTTP fallback in local mode

    # 7. Write metadata
    _write_metadata(version, tags)

    _print_footer()


# ===========================================================================
# REMOTE mode — connects to the MLflow HTTP tracking server
# ===========================================================================

def _remote_sync() -> None:
    """
    Sync via the MLflow HTTP tracking server (original behaviour).
    """
    import mlflow
    from mlflow import MlflowClient

    _print_header("remote / HTTP")
    print(f"MLflow URI       : {MLFLOW_TRACKING_URI}\n")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    # 1. Resolve champion alias
    try:
        mv = client.get_model_version_by_alias(REGISTERED_MODEL_NAME, "champion")
    except Exception as exc:
        print(
            f"ERROR: Could not resolve 'champion' alias for '{REGISTERED_MODEL_NAME}'.\n"
            f"  Make sure the MLflow server is running at {MLFLOW_TRACKING_URI}\n"
            f"  and the retraining pipeline has promoted at least one champion.\n"
            f"  Detail: {exc}\n\n"
            "  Tip: if the server is not running, use --local to sync offline.",
            file=sys.stderr,
        )
        sys.exit(1)

    version = mv.version
    run_id = mv.run_id
    tags = mv.tags or {}

    print(f"Champion found (via HTTP):")
    print(f"  version              : {version}")
    print(f"  run_id               : {run_id}")
    print(f"  trained_up_to_period : {tags.get('trained_up_to_period', 'n/a')}")
    print(f"  decision_threshold   : {tags.get('decision_threshold', 'n/a')}\n")

    # 2. Download model artifact directory → OUTPUT_DIR/model/
    artifact_uri = f"runs:/{run_id}/model"
    print(f"  Downloading model artifact: {artifact_uri}")

    with tempfile.TemporaryDirectory() as tmp:
        local_path = mlflow.artifacts.download_artifacts(
            artifact_uri=artifact_uri,
            dst_path=tmp,
        )
        model_dest = OUTPUT_DIR / "model"
        if model_dest.exists():
            shutil.rmtree(model_dest)
        shutil.copytree(local_path, str(model_dest))

    print(f"  Model saved to: {model_dest}/")

    # 3. Sync feature list (with HTTP fallback)
    _sync_feature_list(run_id=run_id)

    # 4. Write metadata
    _write_metadata(version, tags)

    _print_footer()


# ===========================================================================
# Entry point
# ===========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Sync the current MLflow champion model into "
            "./deploy_to_production/artifacts/current/. "
            "Use --local to sync offline without the MLflow HTTP server."
        )
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help=(
            "Read mlflow.db and copy from mlruns/ directly. "
            "The MLflow HTTP server does NOT need to be running."
        ),
    )
    parser.add_argument(
        "--db",
        default=str(_DEFAULT_DB_PATH),
        metavar="PATH",
        help=f"Path to mlflow.db (default: {_DEFAULT_DB_PATH}). Used only with --local.",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.local:
        _local_sync(db_path=Path(args.db))
    else:
        _remote_sync()


if __name__ == "__main__":
    main()
