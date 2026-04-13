FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ARTIFACTS_DIR=/app/artifacts/current

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App source only — no model artifacts
COPY app.py .

# Mount point for champion artifacts (populated by sync_champion_from_mlflow.py)
RUN mkdir -p /app/artifacts/current

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
