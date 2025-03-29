# ------------- Enable necessary APIs ---------------
resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "composer.googleapis.com",
    "storage.googleapis.com"
  ])
  
  project = var.project_id
  service = each.key
}

# ------------- IAM config ---------------
# Create a service account for Data Pipeline
resource "google_service_account" "data_pipeline_sa" {
  depends_on = [ google_project_service.apis ]
  account_id   = "data-pipeline-sa"
  display_name = "Service Account for Data Pipeline"
}

# Add roles/editor to service accounts
resource "google_project_iam_member" "data_pipeline_sa_role" {
  project = var.project_id
  role    = "roles/editor"
  member  = "serviceAccount:${google_service_account.data_pipeline_sa.email}"
}

data "google_service_account" "service" {
  depends_on = [ google_project_service.apis ]
  account_id = "service"
}

resource "google_project_iam_member" "service_role" {
  project = var.project_id
  role    = "roles/editor"
  member  = "serviceAccount:${data.google_service_account.service.email}"
}

# Create a dependency anchor
resource "null_resource" "iam_group_complete" {
  depends_on = [
    google_project_service.apis,
    google_project_iam_member.service_role,
  ]
}

# ------------- Resources ---------------
# Create a Cloud Storage bucket for Data Lake
resource "google_storage_bucket" "data_lake" {
  depends_on = [null_resource.iam_group_complete]
  name     = var.gcs_bucket_name
  location = var.region
  storage_class = "STANDARD"

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365  # Delete objects older than 1 year
    }
  }
}

# Create a BigQuery dataset
resource "google_bigquery_dataset" "dataset" {
  depends_on = [null_resource.iam_group_complete]
  dataset_id  = var.bq_dataset_name
  project     = var.project_id
  location    = var.region
}

# Grant the required role to Cloud Composer service account
resource "google_project_iam_member" "composer_service_agent_v2" {
  depends_on = [ google_project_service.apis ]
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:${google_composer_environment.airflow.config.0.node_config.0.service_account}"
}

resource "google_project_iam_member" "composer_worker" {
  depends_on = [ google_project_service.apis ]
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_composer_environment.airflow.config.0.node_config.0.service_account}"
}

# Create a Composer environment for Airflow
resource "google_composer_environment" "airflow" {
  depends_on = [null_resource.iam_group_complete]
  name   = var.composer_env_name
  region = var.region

  config {
    software_config {
      image_version = "composer-3-airflow-2"
      
      # Install packages
      pypi_packages = {
        "pandas" = ">=1.3.0",
        "apache-airflow-providers-google" = ">=8.0.0",
        "kagglehub" = ">=0.3.0",
        "dbt-core" = ">=1.0.0",
        "dbt-bigquery" = ">=1.0.0",
        "airflow-dbt-python" = ">=2.0.0"
      }

      # Environment variables
      env_variables = {
        AIRFLOW_PROJECT_ID  = var.project_id
        GCS_BUCKET_NAME     = var.gcs_bucket_name
        BQ_DATASET_NAME     = var.bq_dataset_name
        GCP_REGION          = var.region
        AIRFLOW_CONN_FS_DEFAULT = "file:///tmp"
      }
    }
  }
}

# Upload DAG files to Cloud Storage
locals {
  dag_files = fileset("../02-workflow-orchestration/", "*.py")
  composer_bucket_name = replace(
  replace(
      google_composer_environment.airflow.config.0.dag_gcs_prefix,
      "gs://", ""
  ),
  "/dags", ""
  )
}

resource "google_storage_bucket_object" "dag" {
  for_each = local.dag_files
  name     = "dags/${each.value}"
  source   = "../02-workflow-orchestration/${each.value}"
  content_type = "text/x-python"
  bucket   = local.composer_bucket_name
  detect_md5hash = filemd5("../02-workflow-orchestration/${each.value}")
}

# Copy the entire dbt project directory to Composer's DAGs folder
locals {
  dbt_files = fileset("../03-analytics-engineering/happiness_analytics/", "**")
  excluded_folders = ["dbt_packages/", "target/", "logs/"]
  filtered_dbt_files = [
    for file in local.dbt_files : file
    if !anytrue([for folder in local.excluded_folders : startswith(file, folder)])
  ]
}

resource "google_storage_bucket_object" "dbt" {
  for_each = toset(local.filtered_dbt_files)

  name   = "dags/dbt/${each.value}"
  bucket = local.composer_bucket_name
  source = "../03-analytics-engineering/happiness_analytics/${each.value}"
  content_type = lookup({
    "sql"  = "text/x-sql",
    "yml"  = "text/yaml",
    "yaml" = "text/yaml",
    "md"   = "text/markdown",
    "csv"  = "text/csv"
  }, split(".", each.value)[length(split(".", each.value)) - 1], "application/octet-stream")
  detect_md5hash = filemd5("../03-analytics-engineering/happiness_analytics/${each.value}")
}

# Grant the Composer SA BigQuery access
resource "google_project_iam_member" "composer_bigquery_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_composer_environment.airflow.config.0.node_config.0.service_account}"
}

resource "google_project_iam_member" "composer_bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_composer_environment.airflow.config.0.node_config.0.service_account}"
}