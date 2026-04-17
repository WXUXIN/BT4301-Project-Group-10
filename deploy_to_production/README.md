# Customer Churn — Production Serving Layer

FastAPI backend + ChurnGuard frontend dashboard for the MLflow champion churn model.  
The Docker image contains only app code; the champion model artifacts are mounted from the host at runtime.

---

## Directory layout

```
deploy_to_production/
├── app.py                          # FastAPI serving application
├── index.html                      # ChurnGuard frontend dashboard
├── latest_predictions_input.json   # Customer feature data for scoring
├── sync_champion_from_mlflow.py    # Pulls champion artifacts from MLflow
├── requirements.txt                # Serving container dependencies
├── Dockerfile                      # Image build (no model artifacts baked in)
├── compose.yaml                    # Docker Compose with volume mount
├── .dockerignore
└── artifacts/
    └── current/                    # ← populated by sync script (gitignored)
        ├── model/
        │   ├── model.pkl           # sklearn Pipeline (preprocessor + XGBClassifier)
        │   └── MLmodel
        ├── feature_lists.json
        └── metadata.json
```

---

## ChurnGuard Frontend Dashboard

`index.html` is a single-page dashboard for viewing churn predictions, exploring customer profiles, and drafting retention emails. It calls the FastAPI backend for scoring and falls back to client-side mock scoring automatically if the API is unavailable.

### Running locally

Two terminals, both from `deploy_to_production/`:

```bash
# Terminal 1 — FastAPI backend (requires model artifacts)
pip install -r requirements.txt
ARTIFACTS_DIR=./artifacts/current uvicorn app:app --reload --port 8000

# Terminal 2 — Static file server for the frontend
python3 -m http.server 3000
```

Open **http://localhost:3000**, then click **Run Predictions**.

> **No model artifacts?** The dashboard automatically detects the API is offline and switches to **Demo Mode** (client-side mock scoring). The header pill turns orange. All UI features — filtering, email modal, export — still work.

> **Why a static file server?** `fetch('latest_predictions_input.json')` is blocked by browsers when opening `index.html` directly from the filesystem (`file://`). A local HTTP server (`python3 -m http.server`) bypasses this restriction.

---

### Dashboard features

#### Header bar
Displays the active model name and version, decision threshold, and an API status pill:
- **Green — API Connected**: predictions come from the live FastAPI model
- **Orange — Demo Mode**: API is offline; predictions use client-side heuristic scoring
- **Grey — Not connected**: predictions have not been run yet

#### Summary stat cards
Four cards update whenever the period or filters change:

| Card | What it shows |
|---|---|
| Customers Scored | Total customers in the selected period |
| At Risk | Count and percentage of customers predicted to churn |
| High Risk | Count of `high` risk-tier customers; medium count shown below |
| Avg Churn Prob | Average churn probability across at-risk customers only |

#### Period selector
The 1,142 customers in `latest_predictions_input.json` are split into three equal mock periods to simulate a rolling monthly data pipeline:

| Option | Records |
|---|---|
| Period 3 — Latest *(default)* | Last third (~381 customers) |
| Period 2 | Middle third |
| Period 1 | First third |
| All Periods | All 1,142 customers |

Switching periods instantly re-filters the already-scored predictions — no re-call to the API.

#### Risk filter toggle
- **At-Risk Only** *(default)*: shows only customers where `churn_prediction = 1`
- **All Customers**: shows everyone, including low-risk customers

#### Customer ID search
Live text filter applied on top of the period and risk filters. Useful for looking up a specific customer before sending a retention email.

#### Predictions table
Each row shows:

| Column | Detail |
|---|---|
| Customer ID | Monospace, coloured to stand out |
| Churn Probability | Inline bar + percentage, colour-coded by risk tier |
| Risk Tier | Badge: red = high, orange = medium, green = low |
| Contract | Badge: yellow = Monthly (highest risk), teal = 1-Year, green = 2-Year |
| Tenure Segment | Badge: Infant / Stable / Loyal / Veteran |
| Actions | Email button (at-risk only) + expand toggle |

#### Expandable customer detail row
Click **▼** on any row to reveal a detail panel showing:
- Satisfaction score, complaint count, service call count, late payments — each colour-coded (red if concerning, orange if moderate, green if healthy)
- Churn probability to 2 decimal places
- Whether the `high_risk_flag` is set
- Service subscription chips: Phone, Internet, Tech Support, Streaming TV, Movies, Auto Pay — each shown as active (green) or inactive (grey strikethrough)

#### Retention email modal
The **✉ Email** button appears on every at-risk customer row. Clicking it opens a modal pre-populated with:

- **To**: a mock email address derived from the customer ID
- **Subject** and **Message**: generated based on the customer's risk profile

The message template is chosen by matching against the customer's features in priority order:

| Condition | Message angle |
|---|---|
| High risk + month-to-month contract | Offer 20% discount to switch to annual plan |
| High risk + ≥2 complaints | Apology + service credit + dedicated account manager |
| High risk + ≥1 late payment | Flexible payment plan options |
| High risk + satisfaction ≤ 4 | Personal outreach to understand dissatisfaction |
| High risk (other) | Loyalty rewards + plan review |
| Medium risk | General check-in and plan optimisation offer |

All fields are fully editable before sending. Clicking **Send Email** closes the modal and shows a toast notification — no real email is sent (mock only).

#### Export CSV
Downloads a `.csv` of the currently visible records (respects the active period and risk filter). Columns: `customer_id`, `churn_probability`, `churn_prediction`, `risk_tier`, `contract`, `tenure_segment`, `customer_satisfaction`, `num_complaints`, `num_service_calls`, `late_payments`, `high_risk_flag`.

File is named `churn_<period>_<date>.csv`.

---

## Backend prerequisites

| Requirement | Where |
|---|---|
| Docker + Docker Compose v2 | local machine |
| Python 3.10+ with project venv active | for the sync script |
| MLflow tracking server running | `http://127.0.0.1:9080` (or set `MLFLOW_TRACKING_URI`) |
| At least one promoted champion in MLflow | produced by the retraining pipeline |

Install sync-script dependencies (mlflow is needed only on the host, not in the container):

```bash
# from repo root, with venv active
pip install mlflow
```

> **Tip — sync without the HTTP server:** the `--local` flag reads `mlflow.db` and  
> copies directly from `mlruns/`. No running server required (see §Sync modes below).

---

## Operational workflow

### 1. First-time setup — build the image

```bash
# from the repo root
docker compose -f deploy_to_production/compose.yaml build
```

### 2. Sync the current champion from MLflow

**Option A — remote mode** (MLflow HTTP server must be running):

```bash
python deploy_to_production/sync_champion_from_mlflow.py
```

**Option B — local / offline mode** (no server needed):

```bash
python deploy_to_production/sync_champion_from_mlflow.py --local
```

Local mode queries `mlflow.db` (SQLite) directly and copies `model.pkl` from  
`mlruns/` on disk. The champion alias, storage path, and all tags are read  
straight from the database — identical result to remote mode, zero HTTP calls.

Both options write artifacts into `./deploy_to_production/artifacts/current/`.

### 3. Start the serving container

```bash
docker compose -f deploy_to_production/compose.yaml up -d
```

### 4. Verify the service is running

```bash
# health check
curl http://localhost:8000/health

# Swagger UI (browser)
open http://localhost:8000/docs
```

---

## Updating the champion (after a new retrain promotion)

When the retraining pipeline promotes a new MLflow champion:

```bash
# 1. Sync new champion artifacts to the host
#    (use --local if the MLflow HTTP server is not running)
python deploy_to_production/sync_champion_from_mlflow.py --local

# 2. Restart the container (picks up the new artifacts from the volume)
docker compose -f deploy_to_production/compose.yaml restart churn-api

# 3. Confirm new version is live
curl http://localhost:8000/health
```

## Sync modes

| Mode | Command | When to use |
|------|---------|-------------|
| Remote (HTTP) | `python deploy_to_production/sync_champion_from_mlflow.py` | MLflow server is up |
| Local (offline) | `python deploy_to_production/sync_champion_from_mlflow.py --local` | Server is down / for demos |

Local mode reads `mlflow.db` with Python's built-in `sqlite3` module — no  
extra dependencies — then copies `model.pkl` directly from `mlruns/`.  
The result is identical: `artifacts/current/model/`, `feature_lists.json`,  
`metadata.json`.

Custom database path (if `mlflow.db` is not at the project root):

```bash
python deploy_to_production/sync_champion_from_mlflow.py --local --db /path/to/mlflow.db
```

No image rebuild is needed — the volume mount ensures the container always reads  
whatever is in `./deploy_to_production/artifacts/current/`.

---

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Service info + champion provenance |
| GET | `/health` | Health check + loaded model metadata |
| POST | `/predict` | Score a single customer |
| POST | `/predict-batch` | Score a list of customers |
| GET | `/docs` | Swagger UI |
| GET | `/redoc` | ReDoc UI |

### POST /predict — example request

```bash
curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "customer_satisfaction": 2.5,
    "num_complaints": 3,
    "num_service_calls": 5,
    "late_payments": 2,
    "has_phone_service": 1,
    "has_internet_service": 1,
    "has_tech_support": 0,
    "has_streaming_tv": 1,
    "has_streaming_movies": 0,
    "high_risk_flag": 1,
    "is_auto_pay": 0,
    "tenure_segment": "0_6",
    "contract": "Month-to-month"
  }' | python -m json.tool
```

### POST /predict-batch — example request

```bash
curl -s -X POST http://localhost:8000/predict-batch \
  -H "Content-Type: application/json" \
  -d '[
    {
      "customer_satisfaction": 4.5, "num_complaints": 0,
      "num_service_calls": 1, "late_payments": 0,
      "has_phone_service": 1, "has_internet_service": 1,
      "has_tech_support": 1, "has_streaming_tv": 0,
      "has_streaming_movies": 0, "high_risk_flag": 0,
      "is_auto_pay": 1, "tenure_segment": "24_plus",
      "contract": "Two year"
    },
    {
      "customer_satisfaction": 1.5, "num_complaints": 5,
      "num_service_calls": 8, "late_payments": 3,
      "has_phone_service": 1, "has_internet_service": 1,
      "has_tech_support": 0, "has_streaming_tv": 1,
      "has_streaming_movies": 1, "high_risk_flag": 1,
      "is_auto_pay": 0, "tenure_segment": "0_6",
      "contract": "Month-to-month"
    }
  ]' | python -m json.tool
```

### Response shape

```json
{
  "registered_model_name": "customer_churn_xgb",
  "champion_version": "3",
  "decision_threshold": 0.57,
  "churn_probability": 0.7842,
  "churn_prediction": 1,
  "risk_tier": "high"
}
```

Risk tiers:
- `low` — probability < 0.3
- `medium` — 0.3 ≤ probability < 0.6
- `high` — probability ≥ 0.6

---

## Stopping / removing the container

```bash
docker compose -f deploy_to_production/compose.yaml down
```

---

## Environment variables

| Variable | Default (in container) | Description |
|---|---|---|
| `ARTIFACTS_DIR` | `/app/artifacts/current` | Path to synced champion artifacts |
