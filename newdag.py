from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import boto3

# Define default arguments
default_args = {
    "owner": "airflow",
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 1,
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

# Step 1: Execute the Python script for ETL process
execute_script_task = BashOperator(
    task_id="execute_script",
    bash_command=f"python3 /home/ubuntu/airflow/dags/airflow/py_scripts/python_script.py",
    dag=dag,
)

# Step 2 : Execute bash task
bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "The ETL Script is completed!"',
    dag=dag,
)
execute_script_task3 = BashOperator(
    task_id="execute_script3",
    bash_command='echo "Starting sleep task"; sleep 50; echo "Finished sleep task"',
    dag=dag,
)
# Define task dependencies
[execute_script_task, execute_script_task3] >> bash_task
