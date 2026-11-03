output "gke_cluster_name" {
  value       = google_container_cluster.primary.name
  description = "GKE cluster name"
}

output "gke_endpoint" {
  value       = google_container_cluster.primary.endpoint
  description = "GKE control plane endpoint"
  sensitive   = true
}

output "database_connection_name" {
  value       = google_sql_database_instance.postgres.connection_name
  description = "Cloud SQL database connection name"
}

output "redis_host" {
  value       = google_redis_instance.cache.host
  description = "Cloud Memorystore Redis host"
}

output "ingress_static_ip" {
  value       = google_compute_global_address.ingress_ip.address
  description = "Static external IP address for Ingress"
}

output "workload_service_account_email" {
  value       = google_service_account.app_sa.email
  description = "GCP Service Account email bound to GKE Workload Identity"
}

output "artifact_registry_repo" {
  value       = google_artifact_registry_repository.repo.name
  description = "Artifact Registry Docker repository"
}
