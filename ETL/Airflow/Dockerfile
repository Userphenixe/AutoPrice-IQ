FROM apache/airflow:2.10.5

USER airflow

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir \
    "apache-airflow-providers-postgres>=5.0.0" \
    pandas \
    scikit-learn \
    sqlalchemy \
    psycopg2-binary \
    pytz \
    --constraint "${CONSTRAINT_URL}"

ENV PYTHONPATH="/opt/airflow/etl:${PYTHONPATH}"
