from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "email": ["email@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "process_web_log",
    default_args=default_args,
    description="A dag to process log file",
    schedule_interval=timedelta(days=1),
    catchup=False
)

extract_data = BashOperator(
    task_id="extract_data",
    bash_command="awk '{print $1}' /home/project/airflow/dags/capstone/accesslog.txt > /home/project/airflow/dags/capstone/extracted_data.txt",
    dag=dag
)

transform_data = BashOperator(
    task_id="transform_data",
    bash_command="grep -v '198.46.149.143' /home/project/airflow/dags/capstone/extracted_data.txt > /home/project/airflow/dags/capstone/transformed_data.txt",
    dag=dag
)

load_data = BashOperator(
    task_id='load_data',
    bash_command='tar -cvf /home/project/airflow/dags/capstone/weblog.tar /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag
)

extract_data >> transform_data >> load_data
