FROM apache/airflow:2.10.5

USER root

RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libxkbcommon0 \
    libcups2 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libasound2 \
  && rm -rf /var/lib/apt/lists/*

USER airflow

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
    playwright \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    scikit-learn \
    "apache-airflow-providers-postgres>=5.0.0" \
    --constraint "${CONSTRAINT_URL}"

RUN python -m playwright install chromium
