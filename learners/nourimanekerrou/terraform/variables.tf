variable "project_id" {
  type        = string
  description = "Sandbox project id (the kuikops-lakehouse in the lessons)."
}

variable "handle" {
  type        = string
  description = "Your handle: lowercase letters/digits."
}

variable "service_account" {
  type        = string
  description = "Your batch service account, <handle>-batch@kuikops-lakehouse.iam.gserviceaccount.com."
}

variable "run" {
  type        = number
  description = "Lesson 1 hello batch run number (part of batch_id; batches are immutable)."
}

variable "drop" {
  type        = number
  description = "Which landing drop the bronze batch reads (drop<N>/)."
}

variable "bronze_run" {
  type        = number
  description = "Bronze batch run number."
}

variable "silver_run" {
  type        = number
  description = "Silver batch run number."
}

variable "gold_run" {
  type        = number
  description = "Gold batch run number."
}
