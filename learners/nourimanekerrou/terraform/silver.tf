resource "google_storage_bucket_object" "silver_job" {
  bucket = google_storage_bucket.landing.name
  name   = "jobs/silver.py"
  source = "${path.module}/../jobs/silver.py"
}

resource "google_dataproc_batch" "silver" {
  batch_id = "${var.handle}-silver-${var.silver_run}"
  location = "europe-west1"
  runtime_config {
    version    = "3.0"
    properties = { "spark.dynamicAllocation.maxExecutors" = "2" }
  }
  environment_config {
    execution_config {
      service_account = var.service_account
      ttl             = "1800s"
      authentication_config {
        user_workload_authentication_type = "SERVICE_ACCOUNT"
      }
    }
  }
  pyspark_batch {
    main_python_file_uri = "gs://${google_storage_bucket_object.silver_job.bucket}/${google_storage_bucket_object.silver_job.name}"
    args = [
      "gs://${google_storage_bucket.bronze.name}/products",
      "gs://${google_storage_bucket.silver.name}/products",
      "gs://${google_storage_bucket.silver.name}/quarantine",
    ]
  }
}