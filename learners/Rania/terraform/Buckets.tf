resource "google_storage_bucket" "landing" {
  name                        = "kuikops-lakehouse-${var.handle}-landing"
  location                    = "EUROPE-WEST1"
  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    owner = var.handle
    layer = "landing"
  }
}

resource "google_storage_bucket" "bronze" {
  name                        = "kuikops-lakehouse-${var.handle}-bronze"
  location                    = "EUROPE-WEST1"
  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    owner = var.handle
    layer = "bronze"
  }
}

resource "google_storage_bucket" "silver" {
  name                        = "kuikops-lakehouse-${var.handle}-silver"
  location                    = "EUROPE-WEST1"
  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    owner = var.handle
    layer = "silver"
  }
}

resource "google_storage_bucket" "gold" {
  name                        = "kuikops-lakehouse-${var.handle}-gold"
  location                    = "EUROPE-WEST1"
  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    owner = var.handle
    layer = "gold"
  }
}