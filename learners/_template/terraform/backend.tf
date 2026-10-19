terraform {
  backend "gcs" {
    bucket = "kuikops-lakehouse-tfstate"
    # prefix comes from the command line so the file is identical for everyone
  }
}
