FROM python:3.11-slim

ENV PIP_DEFAULT_TIMEOUT=1000 \
    PIP_RETRIES=10

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --retries 10 --timeout 1000 \
        "mlflow[auth]==3.13.0" \
        psycopg2-binary \
        boto3 \
        gunicorn

WORKDIR /mlflow_data
EXPOSE 5000
