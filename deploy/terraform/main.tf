provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_compute_network" "vpc" {
  name                    = "${var.environment}-dist-tx-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.environment}-dist-tx-subnet"
  region        = var.region
  network       = google_compute_network.vpc.name
  ip_cidr_range = "10.10.0.0/20"

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.20.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

resource "google_compute_global_address" "private_ip_address" {
  name          = "${var.environment}-private-ip-alloc"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address.name]
}

resource "google_container_cluster" "primary" {
  name     = "${var.environment}-dist-tx-gke"
  location = var.region

  remove_default_node_pool = true
  initial_node_count       = 1

  network    = google_compute_network.vpc.name
  subnetwork = google_compute_subnetwork.subnet.name

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
}

resource "google_container_node_pool" "primary_nodes" {
  name       = "${var.environment}-node-pool"
  location   = var.region
  cluster    = google_container_cluster.primary.name
  node_count = var.gke_num_nodes

  node_config {
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    labels = {
      env = var.environment
    }
    machine_type = "e2-standard-4"
    tags         = ["gke-node", "${var.environment}-dist-tx"]
    metadata = {
      disable-legacy-endpoints = "true"
    }
  }
}

resource "google_sql_database_instance" "postgres" {
  name             = "${var.environment}-dist-tx-db"
  database_version = "POSTGRES_17"
  region           = var.region

  settings {
    tier = "db-custom-2-7680"
    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
    }
  }
  deletion_protection = true
  depends_on          = [google_service_networking_connection.private_vpc_connection]
}

resource "google_sql_database" "database" {
  name     = "dist_tx_engine"
  instance = google_sql_database_instance.postgres.name
}

resource "google_redis_instance" "cache" {
  name           = "${var.environment}-dist-tx-cache"
  tier           = "BASIC"
  memory_size_gb = 1
  region         = var.region

  authorized_network = google_compute_network.vpc.id
  redis_version      = "REDIS_7_2"
  depends_on         = [google_service_networking_connection.private_vpc_connection]
}

# Declarative Pub/Sub Messaging Topics per messaging/topics/topics.yaml
resource "google_pubsub_topic" "events" {
  name                       = "distributed-transaction-engine-events"
  message_retention_duration = "604800s" # 7 days (matching retention_ms: 604800000)

  labels = {
    env    = var.environment
    system = "distributed-transaction-engine"
  }
}

resource "google_pubsub_subscription" "events_sub" {
  name  = "distributed-transaction-engine-events-sub"
  topic = google_pubsub_topic.events.name

  ack_deadline_seconds = 30

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.events_dlq.id
    max_delivery_attempts = 5
  }
}

resource "google_pubsub_topic" "events_dlq" {
  name = "distributed-transaction-engine-events-dlq"

  labels = {
    env    = var.environment
    system = "distributed-transaction-engine"
  }
}
