from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define a simple Python function
def hello_world():
    print("Hello, world!")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

# Instantiate the DAG
dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# Define the PythonOperator task
hello_world_task = PythonOperator(
    task_id='print_hello_world',
    python_callable=hello_world,
    dag=dag,
)

# Set the task dependencies (in this simple DAG, there's only one task)
hello_world_task
