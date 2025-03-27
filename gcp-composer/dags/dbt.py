from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pendulum

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 22),
}

# Initialize the DAG
with DAG(
    'ecommerce_dbt',
    default_args=default_args,
    description='dbt transform & test',
    schedule=None,  # Define your schedule
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command="""
        cd /home/airflow/gcs/data/ecommerce_dbt &&
        dbt deps
        """,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
        cd /home/airflow/gcs/data/ecommerce_dbt &&
        dbt run --target prod --profiles-dir . > ../logs/dbt_run.log
        """,
    )