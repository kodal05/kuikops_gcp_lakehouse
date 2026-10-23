resource "google_storage_bucket" "landing" {
  name                        = "${var.project_id}-${var.handle}-landing"
  location                    = "europe-west1"
  uniform_bucket_level_access = true   # IAM on the bucket, no per-object ACLs
  force_destroy               = true   # sandbox: destroy may delete objects

  labels = {
    owner = var.handle
    layer = "landing"
  }
}

resource "google_storage_bucket" "bronze" {
  name                        = "${var.project_id}-${var.handle}-bronze"
  location                    = "europe-west1"
  uniform_bucket_level_access = true   # IAM on the bucket, no per-object ACLs
  force_destroy               = true   # sandbox: destroy may delete objects

  labels = {
    owner = var.handle
    layer = "bronze"
  }
}

resource "google_storage_bucket" "silver" {
  name                        = "${var.project_id}-${var.handle}-silver"
  location                    = "europe-west1"
  uniform_bucket_level_access = true   # IAM on the bucket, no per-object ACLs
  force_destroy               = true   # sandbox: destroy may delete objects

  labels = {
    owner = var.handle
    layer = "silver"
  }
}

resource "google_storage_bucket" "gold" {
  name                        = "${var.project_id}-${var.handle}-gold"
  location                    = "europe-west1"
  uniform_bucket_level_access = true   # IAM on the bucket, no per-object ACLs
  force_destroy               = true   # sandbox: destroy may delete objects

  labels = {
    owner = var.handle
    layer = "gold"
  }
}

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


resource "google_storage_bucket_object" "bronze_job" {
  bucket = google_storage_bucket.landing.name
  name   = "jobs/bronze.py"
  source = "${path.module}/../jobs/bronze.py"
}

resource "google_dataproc_batch" "bronze" {
  batch_id = "${var.handle}-bronze-${var.bronze_run}"
  location = "europe-west1"
  runtime_config {
    version    = "3.0"
    properties = { "spark.dynamicAllocation.maxExecutors" = "2" } # sandbox: drops are KBs, a client's GB a day needs executor sizing (lesson 10, section 5)
  }
  environment_config {
    execution_config {
      service_account = var.service_account
      ttl             = "1800s" # a hung batch is killed after 30 min
      authentication_config {
        user_workload_authentication_type = "SERVICE_ACCOUNT" # the API sets this: declare it or every apply replaces the batch
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