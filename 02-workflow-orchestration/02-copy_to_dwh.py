from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator

import os

# Get environment variables from your Composer configuration
PROJECT_ID = os.environ.get('AIRFLOW_PROJECT_ID')
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
BQ_DATASET_NAME = os.environ.get('BQ_DATASET_NAME')
BQ_TABLE_NAME = "happiness_index"

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

with DAG(
    '02-copy_to_dwh',
    default_args=default_args,
    description='A DAG to create BigQuery table from CSV in GCS',
    schedule_interval=None, # "@once"
    catchup=False,
    tags=['gcs', 'bigquery'],
) as dag:
    
    schema = [
        {"name": "Country", "type": "STRING"},
        {"name": "Year", "type": "INTEGER"},
        {"name": "Headline_Consumer_Price_Inflation", "type": "FLOAT"},
        {"name": "Energy_Consumer_Price_Inflation", "type": "FLOAT"},
        {"name": "Food_Consumer_Price_Inflation", "type": "FLOAT"},
        {"name": "Official_Core_Consumer_Price_Inflation", "type": "FLOAT"},
        {"name": "Producer_Price_Inflation", "type": "FLOAT"},
        {"name": "GDP_Deflator_Index_Growth_Rate", "type": "FLOAT"},
        {"name": "Continent_Region", "type": "STRING"},  # Fixed field name
        {"name": "Score", "type": "FLOAT"},
        {"name": "GDP_per_Capita", "type": "FLOAT"},
        {"name": "Social_Support", "type": "FLOAT"},
        {"name": "Healthy_Life_Expectancy_at_Birth", "type": "FLOAT"},
        {"name": "Freedom_to_Make_Life_Choices", "type": "FLOAT"},
        {"name": "Generosity", "type": "FLOAT"},
        {"name": "Perceptions_of_Corruption", "type": "FLOAT"}
    ]

    # Task 0: Ensure table is deleted before recreation
    delete_bq_table = BigQueryDeleteTableOperator(
        task_id="delete_bq_table",
        deletion_dataset_table=f"{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        ignore_if_missing=True,  # Prevent errors if table does not exist
    )

    # Task 1: Create BigQuery Table with Partitioning & Clustering
    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_table",
        project_id=PROJECT_ID,
        dataset_id=BQ_DATASET_NAME,
        table_id=BQ_TABLE_NAME,
        schema_fields=schema,
        cluster_fields=["Year", "Country", "Continent_Region"],  # Optimize queries
        exists_ok=True,  # Avoid errors if table already exists
    )

    # Task 2: Load CSV from GCS to BigQuery
    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket=GCS_BUCKET_NAME,
        source_objects=['kaggle_data/*.csv'],  # Path to your CSV file(s)
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}',
        schema_fields=schema,
        skip_leading_rows=1,  # If your CSV has headers
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',  # Creates table if it doesn't exist
        write_disposition='WRITE_TRUNCATE',  # Overwrites table if it exists
        field_delimiter=',',
        max_bad_records=10,  # Number of errors to ignore
    )

    # Task Dependencies
    delete_bq_table >> create_bq_table >> load_csv_to_bq