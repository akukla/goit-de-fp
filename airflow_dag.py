from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
import random
import time
import mysql.connector

spark_master = "spark://spark:7077"

db_connection = {
    'user': 'root',
    'password': 'goit',
    'host': '192.168.1.78'
    # 'database': 'olympic_dataset'
}

connection_name = "goit"

# Function to create the table
def create_table():
    # create_database()
    result_connection = db_connection.copy()
    result_connection["database"] = 'alexander'
    conn = mysql.connector.connect(**result_connection)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alexander.medals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# Function to pick a medal
def pick_medal(ti):
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    ti.xcom_push(key='medal', value=medal)

# Function to calculate Bronze medals
def calc_Bronze():
    conn = mysql.connector.connect(**db_connection)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Gold';")
    count = cur.fetchone()[0]
    cur.execute("INSERT INTO alexander.medals (medal_type, count) VALUES ('Bronze', %s);", (count,))
    conn.commit()
    cur.close()
    conn.close()

# Function to calculate Silver medals
def calc_Silver():
    conn = mysql.connector.connect(**db_connection)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Bronze';")
    count = cur.fetchone()[0]
    cur.execute("INSERT INTO alexander.medals (medal_type, count) VALUES ('Silver', %s);", (count,))
    conn.commit()
    cur.close()
    conn.close()

# Function to calculate Gold medals
def calc_Gold():
    conn = mysql.connector.connect(**db_connection)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Silver';")
    count = cur.fetchone()[0]
    cur.execute("INSERT INTO alexander.medals (medal_type, count) VALUES ('Gold', %s);", (count,))
    conn.commit()
    cur.close()
    conn.close()

# Function to generate delay
def generate_delay():
    time.sleep(5)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

with DAG(
        'medal_count_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        tags=["alexander"]
) as dag:
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    pick_medal_task = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal,
    )

    branch_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=lambda ti: f"calc_{ti.xcom_pull(key='medal')}",
    )

    calc_Bronze_task = PythonOperator(
        task_id='calc_Bronze',
        python_callable=calc_Bronze,
    )

    calc_Silver_task = PythonOperator(
        task_id='calc_Silver',
        python_callable=calc_Silver,
    )

    calc_Gold_task = PythonOperator(
        task_id='calc_Gold',
        python_callable=calc_Gold,
    )

    delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    check_for_correctness_task = SqlSensor(
        task_id='check_for_correctness',
        conn_id='goit',
        sql="SELECT 1 FROM alexander.medals WHERE created_at > NOW() - INTERVAL 30 SECOND ORDER BY created_at DESC LIMIT 1;",
        mode='poke',
        timeout=60,
        poke_interval=10,
    )

    end_task = DummyOperator(
        task_id='end_task'
    )

    # Set dependencies
    create_table_task >> pick_medal_task >> branch_task
    branch_task >> [calc_Bronze_task, calc_Silver_task, calc_Gold_task]
    [calc_Bronze_task, calc_Silver_task, calc_Gold_task] >> delay_task >> check_for_correctness_task >> end_task