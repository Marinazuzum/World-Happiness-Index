from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import kagglehub

# Get environment variables from your Composer configuration
PROJECT_ID = os.environ.get('AIRFLOW_PROJECT_ID')
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
BQ_DATASET_NAME = os.environ.get('BQ_DATASET_NAME')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

def download_kaggle_dataset(dataset_name):
    """
    Download dataset from Kaggle via Kaggle Hub
    """
    
    # Download dataset
    download_path = kagglehub.dataset_download(dataset_name)
     
    return download_path

with DAG(
    '01-ingest_to_datalake',
    default_args=default_args,
    description='A DAG to download dataset from Kaggle and upload to GCS',
    schedule_interval=None, # "@once"
    catchup=False,
    tags=['kaggle', 'gcs'],
) as dag:

    # Task to download dataset from Kaggle
    download_dataset = PythonOperator(
        task_id='download_kaggle_dataset',
        python_callable=download_kaggle_dataset,
        op_kwargs={
            'dataset_name': 'agrafintech/world-happiness-index-and-inflation-dataset'
        },
    )

    # Task to upload the downloaded files to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='{{ ti.xcom_pull(task_ids="download_kaggle_dataset") }}/*',  # Directly use the returned path
        dst='kaggle_data/',  # Destination path in GCS
        bucket=GCS_BUCKET_NAME,
    )

    # Set task dependencies
    download_dataset >> upload_to_gcs