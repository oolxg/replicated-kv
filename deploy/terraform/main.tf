# Topology: node_count storage VMs + 1 router VM + 1 k6 loadgen VM in one
# subnet. Static membership: internal IPs are assigned deterministically from
# the subnet range BEFORE the instances exist, so every startup script can
# receive the full node list — no discovery at runtime (deliberate design
# limit, documented in the README).

locals {
  subnet_cidr = "10.10.0.0/24"

  # .0/.1 are GCP-reserved; keep low addresses for the fixed roles and start
  # storage nodes at .10.
  loadgen_ip  = cidrhost(local.subnet_cidr, 4)
  router_ip   = cidrhost(local.subnet_cidr, 5)
  storage_ips = [for i in range(var.node_count) : cidrhost(local.subnet_cidr, 10 + i)]

  kv_nodes = join(",", [for ip in local.storage_ips : "${ip}:${var.kv_port}"])

  shed_env = compact([
    var.kv_shed_concurrent == "" ? "" : "KV_SHED_CONCURRENT=${var.kv_shed_concurrent}",
    var.kv_shed_queue == "" ? "" : "KV_SHED_QUEUE=${var.kv_shed_queue}",
  ])

  storage_env = join("\n", concat(["KV_ADDR=:${var.kv_port}"], local.shed_env))

  router_env = join("\n", concat(
    ["KV_ADDR=:${var.kv_port}", "KV_NODES=${local.kv_nodes}"],
    compact([
      var.kv_rf == "" ? "" : "KV_RF=${var.kv_rf}",
      var.kv_w == "" ? "" : "KV_W=${var.kv_w}",
      var.kv_r == "" ? "" : "KV_R=${var.kv_r}",
    ]),
    local.shed_env,
  ))

  image = "debian-cloud/debian-12"
}

# --- network ---

resource "google_compute_network" "kv" {
  name                    = "kv-net"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "kv" {
  name          = "kv-subnet"
  network       = google_compute_network.kv.id
  ip_cidr_range = local.subnet_cidr
}

# Storage nodes talk to each other and to the router only inside the subnet.
resource "google_compute_firewall" "internal" {
  name    = "kv-internal"
  network = google_compute_network.kv.name

  allow {
    protocol = "tcp"
    ports    = [var.kv_port]
  }
  source_ranges = [local.subnet_cidr]
}

# Clients (demo curl, grading) may reach the router's public port.
resource "google_compute_firewall" "client" {
  name    = "kv-client"
  network = google_compute_network.kv.name

  allow {
    protocol = "tcp"
    ports    = [var.kv_port]
  }
  source_ranges = [var.client_cidr]
  target_tags   = ["kv-router"]
}

resource "google_compute_firewall" "ssh" {
  name    = "kv-ssh"
  network = google_compute_network.kv.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = [var.client_cidr]
}

# --- binary distribution: local cross-compiled binaries -> GCS -> VMs ---

resource "google_storage_bucket" "bin" {
  name                        = "${var.project}-kv-bin"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_storage_bucket_object" "storage_bin" {
  name   = "storage"
  bucket = google_storage_bucket.bin.name
  source = "${path.module}/../../bin/linux_amd64/storage"
}

resource "google_storage_bucket_object" "router_bin" {
  name   = "router"
  bucket = google_storage_bucket.bin.name
  source = "${path.module}/../../bin/linux_amd64/router"
}

# --- instances ---

resource "google_compute_instance" "storage" {
  count        = var.node_count
  name         = "kv-storage-${count.index}"
  machine_type = var.machine_type
  tags         = ["kv-node"]

  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = local.image
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.kv.id
    network_ip = local.storage_ips[count.index]
    access_config {} # ephemeral public IP: SSH + GCS egress without NAT setup
  }

  metadata = {
    startup-script = templatefile("${path.module}/templates/service.sh.tpl", {
      bucket    = google_storage_bucket.bin.name
      binary    = "storage"
      env       = local.storage_env
      cpu_quota = var.storage_cpu_quota
    })
    binary-md5 = google_storage_bucket_object.storage_bin.md5hash # replace VM when the binary changes
  }

  service_account {
    scopes = ["storage-ro"]
  }
}

# The router only reads its env at boot, so any change to the rendered config
# (e.g. a different node_count changing KV_NODES) must recreate the VM instead
# of silently updating metadata that will never be re-executed.
resource "terraform_data" "router_cfg" {
  input = local.router_env
}

resource "google_compute_instance" "router" {
  name         = "kv-router"
  machine_type = var.router_machine_type
  tags         = ["kv-router"]

  # Machine-type changes stop/start the VM instead of failing the apply.
  allow_stopping_for_update = true

  lifecycle {
    replace_triggered_by = [terraform_data.router_cfg]
  }

  boot_disk {
    initialize_params {
      image = local.image
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.kv.id
    network_ip = local.router_ip
    access_config {}
  }

  metadata = {
    startup-script = templatefile("${path.module}/templates/service.sh.tpl", {
      bucket    = google_storage_bucket.bin.name
      binary    = "router"
      env       = local.router_env
      cpu_quota = "" # the router is never artificially capped
    })
    binary-md5 = google_storage_bucket_object.router_bin.md5hash
  }

  service_account {
    scopes = ["storage-ro"]
  }
}

resource "google_compute_instance" "loadgen" {
  count        = var.loadgen_enabled ? 1 : 0
  name         = "kv-loadgen"
  machine_type = var.loadgen_machine_type
  tags         = ["kv-loadgen"]

  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = local.image
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.kv.id
    network_ip = local.loadgen_ip
    access_config {}
  }

  metadata = {
    startup-script = file("${path.module}/templates/loadgen.sh")
  }
}
