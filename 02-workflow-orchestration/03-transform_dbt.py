from airflow import DAG
from airflow_dbt_python.operators.dbt import DbtDepsOperator, DbtRunOperator, DbtTestOperator
from airflow.operators.python import PythonOperator
import shutil
import os
from datetime import datetime, timedelta

# Paths
DBT_PROJECT_DIR = "/home/airflow/gcs/dags/dbt"  # Mapped from GCS
LOCAL_DB_PROJECT_DIR = "/tmp/dbt_project"  # Temporary local directory

def copy_dbt_project():
    """Copy the dbt project from the GCS-mounted directory to a temporary location."""
    if os.path.exists(LOCAL_DB_PROJECT_DIR):
        shutil.rmtree(LOCAL_DB_PROJECT_DIR)  # Ensure a clean copy
    shutil.copytree(DBT_PROJECT_DIR, LOCAL_DB_PROJECT_DIR)

def cleanup_local_copy():
    """Delete the local dbt project after execution"""
    shutil.rmtree(LOCAL_DB_PROJECT_DIR, ignore_errors=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '03-transform_dbt',
    default_args=default_args,
    description='A simple DAG to run dbt',
    schedule_interval=None, # "@once",
    catchup=False,
    tags=['dbt', 'bigquery'],
) as dag:

    # Step 1: Copy dbt project to a local directory
    copy_project = PythonOperator(
        task_id="copy_dbt_project",
        python_callable=copy_dbt_project,
    )

    # Step 2: Run dbt deps (install dependencies)
    dbt_deps = DbtDepsOperator(
        task_id="dbt_deps",
        project_dir=LOCAL_DB_PROJECT_DIR,
    )

    # Step 3: Run dbt models
    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        project_dir=LOCAL_DB_PROJECT_DIR,
    )

    # Step 4: Run dbt tests
    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        project_dir=LOCAL_DB_PROJECT_DIR,
    )

    # Step 5: Cleanup local files
    cleanup_task = PythonOperator(
        task_id="cleanup_local_copy",
        python_callable=cleanup_local_copy,
    )

    # Define task dependencies
    copy_project >> dbt_deps >> dbt_run >> dbt_test >> cleanup_task
