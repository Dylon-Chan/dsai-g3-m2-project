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

    # Task 1: Clean targets
    dbt_clean = BashOperator(
        task_id='dbt_clean',
        bash_command="""
        cd /home/airflow/gcs/data/e-commerce-dbt &&
        dbt clean
        """,
    )

    # Task 2: Install dbt packages
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command="""
        cd /home/airflow/gcs/data/e-commerce-dbt &&
        dbt deps
        """,
    )

    # Task 3: Build dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
        cd /home/airflow/gcs/data/e-commerce-dbt &&
        dbt run --target prod --profiles-dir . > ../dbt-logs/dbt_run.log
        """,
    )

    # Task 4: Test dbt models and schemas
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        cd /home/airflow/gcs/data/e-commerce-dbt &&
        dbt test --target prod --profiles-dir . > ../dbt-logs/dbt_test.log
        """,
    )

    # Task Dependencies
    dbt_clean >> dbt_deps >> dbt_run >> dbt_test