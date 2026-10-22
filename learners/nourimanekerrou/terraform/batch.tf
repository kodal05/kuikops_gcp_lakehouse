resource "google_storage_bucket_object" "hello" {
  bucket = google_storage_bucket.landing.name
  name   = "jobs/hello.py"
  source = "${path.module}/../jobs/hello.py"
}

resource "google_dataproc_batch" "hello" {
  batch_id = "${var.handle}-hello-${var.run}"
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
    main_python_file_uri = "gs://${google_storage_bucket_object.hello.bucket}/${google_storage_bucket_object.hello.name}"
    args                 = ["gs://${google_storage_bucket.bronze.name}"]
  }
}
