terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  #credentials = file(var.credentials) 
  #Choose to default to the env variable GOOGLE_APPLICATION_CREDENTIALS
  project = var.project
  region  = var.region
}


resource "google_storage_bucket" "cr-demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}



resource "google_bigquery_dataset" "cr-demo_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}