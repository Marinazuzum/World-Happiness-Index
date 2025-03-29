output "gcs_bucket_url" {
  value = google_storage_bucket.data_lake.url
}

output "bq_dataset_id" {
  value = google_bigquery_dataset.dataset.dataset_id
}

output "composer_airflow_uri" {
  value = google_composer_environment.airflow.config.0.airflow_uri
}
