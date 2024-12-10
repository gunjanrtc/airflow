from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import boto3

# Define default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "my-test-dag",
    default_args=default_args,
    description="Execute a Python script stored in S3",
    schedule_interval=None,
    start_date=datetime(2024, 11, 19),
    catchup=False,
)

# Step 1: Execute the Python script with input
execute_script_task = BashOperator(
    task_id="execute_script",
    bash_command='echo "Starting sleep task"; sleep 50; echo "Finished sleep task"',
    dag=dag,
)

# Define task dependencies
execute_script_task