resource "google_storage_bucket" "landing" {
  name                        = "${var.project_id}-${var.handle}-landing"
  location                    = "europe-west1"
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket" "bronze" {
  name                        = "${var.project_id}-${var.handle}-bronze"
  location                    = "europe-west1"
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket" "silver" {
  name                        = "${var.project_id}-${var.handle}-silver"
  location                    = "europe-west1"
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket" "gold" {
  name                        = "${var.project_id}-${var.handle}-gold"
  location                    = "europe-west1"
  uniform_bucket_level_access = true
  force_destroy               = true
}
