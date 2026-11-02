variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP region"
}

variable "environment" {
  type        = string
  default     = "production"
  description = "Deployment environment (dev, staging, production)"
}

variable "gke_num_nodes" {
  type        = number
  default     = 3
  description = "Number of GKE nodes"
}
