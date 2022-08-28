"""Define Airflow DAG for running"""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators import python_operator
from etl.pipeline import get_connection, run_pipeline


def run_etl():
    """Run ETL process"""

    conn = get_connection()
    run_pipeline(connection=conn)


default_args = {
    "owner": "masum",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email": ["billalmasum93@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}
dag = DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    schedule_interval="12 5 * * *",
)
etl_task = python_operator.PythonOperator(
    task_id="etl_task",
    python_callable=run_etl,
    dag=dag,
)
run_etl()
