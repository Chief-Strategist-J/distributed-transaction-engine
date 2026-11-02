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
