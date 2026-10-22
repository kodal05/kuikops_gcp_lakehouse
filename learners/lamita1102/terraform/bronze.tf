resource "google_storage_bucket_object" "bronze_job" {
  bucket = google_storage_bucket.landing.name
  name   = "jobs/bronze.py"
  source = "${path.module}/../jobs/bronze.py"
}

resource "google_dataproc_batch" "bronze" {
  batch_id = "${var.handle}-bronze-${var.bronze_run}"
  location = "europe-west1"

  runtime_config {
    version = "3.0"

    properties = {
      "spark.dynamicAllocation.maxExecutors" = "2"
    }
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
    main_python_file_uri = "gs://${google_storage_bucket_object.bronze_job.bucket}/${google_storage_bucket_object.bronze_job.name}"

    args = [
      "gs://${google_storage_bucket.landing.name}/drop${var.drop}/",
      "gs://${google_storage_bucket.bronze.name}/products",
    ]
  }
}
