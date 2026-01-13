from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ETL.load_prices import main


with DAG(
    dag_id='crypto_prices_hourly',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    load_prices = PythonOperator(
        task_id='load_crypto_prices',
        python_callable=main,
    )
