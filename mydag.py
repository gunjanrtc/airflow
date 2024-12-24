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
execute_script_task1 = BashOperator(
    task_id="execute_script1",
    bash_command='echo "Starting sleep task"; sleep 50; echo "Finished sleep task"',
    dag=dag,
)
execute_script_task2 = BashOperator(
    task_id="execute_script2",
    bash_command='echo "Starting sleep task"; sleep 50; echo "Finished sleep task"',
    dag=dag,
)
execute_script_task3 = BashOperator(
    task_id="execute_script3",
    bash_command='echo "Starting sleep task"; sleep 50; echo "Finished sleep task"',
    dag=dag,
)

# Define task dependencies
execute_script_task1 >> execute_script_task2 << execute_script_task3
