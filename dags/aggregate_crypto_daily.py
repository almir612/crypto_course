from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ETL.aggregate_daily_prices import main


with DAG(
    dag_id='crypto_daily_avg',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    aggregate = PythonOperator(
        task_id='aggregate_daily_prices',
        python_callable=main,
    )

