variable "credentials" {
  description = "My Credentials"
  default     = "./credentials/gcp_key.json"
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "data-camp-2025"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "europe-west3"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage Bucket for Data Lake"
  type        = string
  default     = "happiness-data-lake"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset for storing processed data"
  type        = string
  default     = "happiness_analysis"
}

variable "composer_env_name" {
  description = "Cloud Composer Environment Name"
  type        = string
  default     = "happiness-airflow"
}
