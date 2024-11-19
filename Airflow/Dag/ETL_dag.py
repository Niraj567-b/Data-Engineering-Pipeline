from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "Niraj",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ETL_dag",
    default_args=default_args,
    description="A Dag to Run ETL Process",
    schedule_interval="0 12 1 * *",  # Will run the first day of every month at 12:00 PM
    start_date=datetime(2024, 11, 19),
    catchup=False,
)

run_extraction = BashOperator(
    task_id="run_extraction",
    bash_command="python /workspaces/Data-Engineering-Pipeline/Script/Data_Extraction.py",
    dag=dag,
)

run_transformation_load = BashOperator(
    task_id="run_transformation_load",
    bash_command="python /workspaces/Data-Engineering-Pipeline/Script/Data_Transformation_Load.py",
    dag=dag,
)

run_extraction >> run_transformation_load
