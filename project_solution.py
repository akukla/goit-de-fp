from airflow import DAG
from datetime import datetime, timedelta
import subprocess
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_script(script_name):
    subprocess.run(['python3', script_name], check=True)


with DAG(
    'data_pipeline',
    default_args=default_args,
    description='A simple data pipeline DAG',
    schedule_interval=None,
    catchup=False,
) as dag:

    task_landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application="/opt/airflow/dags/landing_to_bronze.py", # Spark application path created in airflow and spark cluster
        name="landing_to_bronze",
        verbose=1,
        conn_id="spark_default",
        )

    task_bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application="/opt/airflow/dags/bronze_to_silver.py", # Spark application path created in airflow and spark cluster
        name="bronze_to_silver",
        conn_id="spark_default",
        verbose=1
    )

    task_silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application="/opt/airflow/dags/silver_to_gold.py", # Spark application path created in airflow and spark cluster
        name="silver_to_gold",
        conn_id="spark_default",
        verbose=1
    )

    task_landing_to_bronze >> task_bronze_to_silver >> task_silver_to_gold