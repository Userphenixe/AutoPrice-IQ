from __future__ import annotations

from datetime import datetime, timedelta
import asyncio

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

# Grâce à PYTHONPATH=/opt/airflow/etl, on peut faire ces imports direct
from Extract import aramisauto, autoeasy
from Transform.transform_auto import run_transform
from Load.load_to_pg import load_csv_to_ads


# ========= WRAPPERS SYNCHRONES POUR AIRFLOW =========

def run_aramisauto_task(**context):
    aramisauto.run_aramisauto()

def run_autoeasy_task(**context):
    autoeasy.run_autoeasy()



def run_transform_task(**context):
    run_transform()


def run_load_task(**context):
    load_csv_to_ads(truncate_first=False)


# ================== DÉFINITION DU DAG ==================

default_args = {
    "owner": "hamza",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_autoprice_iq",
    default_args=default_args,
    start_date=datetime(2025, 11, 22),
    schedule_interval="@daily",
    catchup=False,
    description="ETL AutoPrice-IQ : extract (2 sites) -> transform -> load PostgreSQL",
    tags=["autoprice", "etl", "cars"],
) as dag:



    t_extract_aramisauto = PythonOperator(
        task_id="extract_aramisauto",
        python_callable=run_aramisauto_task,
    )

    t_extract_autoeasy = PythonOperator(
        task_id="extract_autoeasy",
        python_callable=run_autoeasy_task,
    )

    t_transform = PythonOperator(
        task_id="transform_auto_csv",
        python_callable=run_transform_task,
    )

    t_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_load_task,
    )

    # 2 extracts en parallèle -> transform -> load
    [t_extract_aramisauto, t_extract_autoeasy] >> t_transform >> t_load
