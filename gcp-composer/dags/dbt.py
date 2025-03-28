from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pendulum
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

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
        dbt run --target prod --profiles-dir .
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

    def get_log_tail(file, **kwargs):
        try:
            with open(f"/home/airflow/gcs/data/dbt-logs/{file}.log") as f:
                lines = f.readlines()
            tail = "\n".join(lines[-100:])
        except FileNotFoundError:
            tail = "[Log file not found]"
        kwargs['ti'].xcom_push(key='log_tail', value=tail)

    # Task 5a: Extract dbt_test log if test failed
    extract_dbt_test_log = PythonOperator(
        task_id="extract_dbt_test_log",
        python_callable=get_log_tail,
        provide_context=True,
        op_kwargs={"file": "dbt_test"},
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # Task 5b: Send email notification if test failed
    notify_dbt_test = EmailOperator(
        task_id='notify_dbt_test',
        to=['wengsiong22@gmail.com'],
        subject='DBT Test Failed at {{ execution_date.in_timezone("Asia/Singapore").strftime("%Y-%m-%d %H:%M:%S") }}',
        html_content="""
            <p>The dbt test task failed.</p>
            <p><b>Here are the last 100 lines of the test log:</b></p>
            <pre>{{ ti.xcom_pull(task_ids='extract_dbt_test_log', key='log_tail') }}</pre>
            <p>Full logs: <a href="https://storage.cloud.google.com/composer-brazilian-ecommerce-bucket/data/dbt-logs/dbt_test.log">View in GCS</a></p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # Task 6: Build DBT documentations static site
    dbt_docs = BashOperator(
        task_id='dbt_docs',
        bash_command="""
        cd /home/airflow/gcs/data/e-commerce-dbt &&
        dbt docs generate --target prod --static --profiles-dir .
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Task 7: Host the DBT docs site in GCS bucket
    host_dbt_docs = GCSToGCSOperator(
        task_id='host_dbt_docs',
        source_bucket='composer-brazilian-ecommerce-bucket',
        source_object='data/e-commerce-dbt/target/*',
        destination_bucket='brazilian-ecommerce-dbt-docs',
        destination_object='target/',
    )

    # Task Dependencies
    dbt_clean >> dbt_deps >> dbt_run >> dbt_test >> extract_dbt_test_log
    dbt_test >> dbt_docs >> host_dbt_docs
    [dbt_test, extract_dbt_test_log] >> notify_dbt_test