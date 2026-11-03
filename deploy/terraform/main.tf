provider "google" {
  project = var.project_id
  region  = var.region
}

# ------------------------------------------------------------------------------
# 1. VPC & CUSTOM SUBNETWORK
# ------------------------------------------------------------------------------
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

  private_ip_google_access = true
}

# ------------------------------------------------------------------------------
# 2. CLOUD ROUTER, NAT & DEFAULT EGRESS ROUTE (Outbound Internet for Private Nodes)
# ------------------------------------------------------------------------------
resource "google_compute_router" "router" {
  name    = "${var.environment}-dist-tx-router"
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "${var.environment}-dist-tx-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  min_ports_per_vm                    = 64
  enable_endpoint_independent_mapping = true

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# ------------------------------------------------------------------------------
# 3. FIREWALL RULES HIERARCHY
# ------------------------------------------------------------------------------
# Rule A: Allow internal communication across Nodes, Pods, and Services
resource "google_compute_firewall" "allow_internal" {
  name    = "${var.environment}-allow-internal-traffic"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = [
    "10.10.0.0/20", # Node VMs
    "10.20.0.0/16", # Pods
    "10.30.0.0/20", # Services
    "10.0.0.0/16"   # Peered Cloud SQL / Redis
  ]
}

# Rule B: Allow Google Cloud Health Checks and Load Balancer Probes
resource "google_compute_firewall" "allow_health_checks" {
  name    = "${var.environment}-allow-gcp-health-checks"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["8080", "443", "80", "13133"]
  }

  source_ranges = [
    "130.211.0.0/22", # Google Cloud External/Internal HTTP(S) LB probes
    "35.191.0.0/16"   # Google Cloud Load Balancer probe IPs
  ]

  target_tags = ["gke-node", "${var.environment}-dist-tx"]
}

# Rule C: Allow Identity-Aware Proxy (IAP) for Secure SSH to Bastion Host
resource "google_compute_firewall" "allow_iap_ssh" {
  name    = "${var.environment}-allow-iap-ssh"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = [
    "35.235.240.0/20" # Google Cloud IAP Netblock
  ]

  target_tags = ["bastion-host"]
}

# Rule D: Allow GKE Master Control Plane to Worker Node Webhooks & Kubelet
resource "google_compute_firewall" "allow_gke_master_to_nodes" {
  name    = "${var.environment}-allow-gke-master-to-nodes"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["443", "10250", "8443", "9443"]
  }

  source_ranges = [
    "172.16.0.0/28" # GKE Master control-plane CIDR block
  ]

  target_tags = ["gke-node", "${var.environment}-dist-tx"]
}

# ------------------------------------------------------------------------------
# 4. PRIVATE SERVICE ACCESS (Cloud SQL & Memorystore Redis Peering)
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# 5. GKE CLUSTER & CONTAINER-NATIVE LOAD BALANCING (NEGs)
# ------------------------------------------------------------------------------
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

  network_policy {
    enabled  = true
    provider = "CALICO"
  }

  addons_config {
    http_load_balancing {
      disabled = false # Enables GKE Ingress controller & Network Endpoint Groups (NEG)
    }
    network_policy_config {
      disabled = false
    }
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }
}

# ------------------------------------------------------------------------------
# 6. GKE AUTOSCALING NODE POOL (Surge & Scaling Configuration)
# ------------------------------------------------------------------------------
resource "google_container_node_pool" "primary_nodes" {
  name     = "${var.environment}-node-pool"
  location = var.region
  cluster  = google_container_cluster.primary.name

  autoscaling {
    min_node_count = 2
    max_node_count = 10
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

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

# ------------------------------------------------------------------------------
# 7. COMPUTE ENGINE SECURE BASTION HOST (IAP-Only Private Access)
# ------------------------------------------------------------------------------
resource "google_service_account" "bastion_sa" {
  account_id   = "${var.environment}-bastion-sa"
  display_name = "Compute Engine Bastion Host Service Account"
}

resource "google_compute_instance" "bastion" {
  name         = "${var.environment}-bastion-host"
  machine_type = "e2-micro"
  zone         = "${var.region}-a"

  tags = ["bastion-host", "${var.environment}-dist-tx"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20
      type  = "pd-standard"
    }
  }

  network_interface {
    network    = google_compute_network.vpc.name
    subnetwork = google_compute_subnetwork.subnet.name
    # Zero external public IP: reachable ONLY through Identity-Aware Proxy (IAP)
  }

  service_account {
    email  = google_service_account.bastion_sa.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    enable-oslogin = "TRUE"
  }
}

# ------------------------------------------------------------------------------
# 8. DATASTORES: PRIVATE CLOUD SQL POSTGRESQL 17 & MEMORYSTORE REDIS
# ------------------------------------------------------------------------------
resource "random_password" "db_password" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "google_sql_database_instance" "postgres" {
  name             = "${var.environment}-dist-tx-db"
  database_version = "POSTGRES_17"
  region           = var.region

  settings {
    tier              = "db-custom-2-7680"
    disk_type         = "PD_SSD"
    disk_autoresize   = true
    availability_type = var.environment == "prod" || var.environment == "production" ? "REGIONAL" : "ZONAL"

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      start_time                     = "03:00"
      transaction_log_retention_days = 7
      backup_retention_settings {
        retained_backups = 14
        retention_unit   = "COUNT"
      }
    }

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
    }
  }

  deletion_protection = var.environment == "prod" || var.environment == "production" ? true : false
  depends_on          = [google_service_networking_connection.private_vpc_connection]
}

resource "google_sql_database" "database" {
  name     = "dist_tx_engine"
  instance = google_sql_database_instance.postgres.name
}

resource "google_sql_user" "db_user" {
  name     = "postgres"
  instance = google_sql_database_instance.postgres.name
  password = random_password.db_password.result
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

# ------------------------------------------------------------------------------
# 9. ASYNCHRONOUS EVENT STREAMING: CLOUD PUB/SUB & DLQ
# ------------------------------------------------------------------------------
resource "google_pubsub_topic" "events" {
  name                       = "distributed-transaction-engine-events"
  message_retention_duration = "604800s" # 7 days

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

# ------------------------------------------------------------------------------
# 10. ARTIFACT REGISTRY & SECRET MANAGER (Missing Critical Infrastructure)
# ------------------------------------------------------------------------------
resource "google_artifact_registry_repository" "repo" {
  location      = var.region
  repository_id = "${var.environment}-dist-tx-repo"
  description   = "Docker container repository for Distributed Transaction Engine"
  format        = "DOCKER"
}

resource "google_secret_manager_secret" "db_password" {
  secret_id = "${var.environment}-dist-tx-db-password"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "db_password" {
  secret      = google_secret_manager_secret.db_password.id
  secret_data = random_password.db_password.result
}

# ------------------------------------------------------------------------------
# 11. WORKLOAD IDENTITY SERVICE ACCOUNT & IAM BINDINGS
# ------------------------------------------------------------------------------
resource "google_service_account" "app_sa" {
  account_id   = "${var.environment}-dist-tx-app"
  display_name = "Workload Identity Service Account for Distributed Transaction Engine"
}

resource "google_service_account_iam_member" "workload_identity_user" {
  service_account_id = google_service_account.app_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[dist-tx-engine/app]"
}

# Grant App SA permission to read DB password from Secret Manager
resource "google_secret_manager_secret_iam_member" "app_db_password" {
  secret_id = google_secret_manager_secret.db_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.app_sa.email}"
}

# Grant App SA permission to publish & subscribe to PubSub events
resource "google_pubsub_topic_iam_member" "app_pubsub_publisher" {
  topic  = google_pubsub_topic.events.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.app_sa.email}"
}

resource "google_pubsub_subscription_iam_member" "app_pubsub_subscriber" {
  subscription = google_pubsub_subscription.events_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.app_sa.email}"
}

# ------------------------------------------------------------------------------
# 12. INGRESS & NETWORKING: STATIC GLOBAL IP FOR EXTERNAL LOAD BALANCER
# ------------------------------------------------------------------------------
resource "google_compute_global_address" "ingress_ip" {
  name        = "${var.environment}-dist-tx-ingress-ip"
  description = "Reserved Global Static IP for external GKE Ingress controller"
}
