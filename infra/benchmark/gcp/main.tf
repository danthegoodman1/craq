data "google_compute_image" "ubuntu_arm64" {
  family  = "ubuntu-2404-lts-arm64"
  project = "ubuntu-os-cloud"
}

data "google_compute_zones" "available" {
  project = var.gcp_project
  region  = var.gcp_region
  status  = "UP"
}

locals {
  multi_zone = var.topology == "multi-zone"
  zones      = slice(data.google_compute_zones.available.names, 0, local.multi_zone ? 3 : 1)

  roles = {
    client = {
      machine_type  = var.client_machine_type
      role          = "client"
      subnet_index  = local.multi_zone && var.client_placement == "remote-zone" ? 1 : 0
      public_ip     = true
      boot_disk_gib = 50
    }
    coordinator = {
      machine_type  = var.coordinator_machine_type
      role          = "coordinator"
      subnet_index  = 0
      public_ip     = false
      boot_disk_gib = var.coordinator_boot_disk_gib
    }
    storage-a = {
      machine_type  = var.storage_machine_type
      role          = "storage"
      subnet_index  = 0
      public_ip     = false
      boot_disk_gib = 80
    }
    storage-b = {
      machine_type  = var.storage_machine_type
      role          = "storage"
      subnet_index  = local.multi_zone ? 1 : 0
      public_ip     = false
      boot_disk_gib = 80
    }
    storage-c = {
      machine_type  = var.storage_machine_type
      role          = "storage"
      subnet_index  = local.multi_zone ? 2 : 0
      public_ip     = false
      boot_disk_gib = 80
    }
  }
}

resource "google_compute_network" "bench" {
  name                    = "craq-bench-${var.run_id}"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "bench" {
  count         = length(local.zones)
  name          = "craq-bench-${var.run_id}-${local.zones[count.index]}"
  ip_cidr_range = cidrsubnet("10.42.0.0/16", 8, count.index)
  network       = google_compute_network.bench.id
  region        = var.gcp_region
}

resource "google_compute_firewall" "operator_ssh" {
  name          = "craq-bench-${var.run_id}-operator-ssh"
  network       = google_compute_network.bench.name
  source_ranges = var.operator_cidrs
  target_tags   = ["craq-bench"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "cluster_internal" {
  name          = "craq-bench-${var.run_id}-cluster-internal"
  network       = google_compute_network.bench.name
  source_ranges = ["10.42.0.0/16"]
  target_tags   = ["craq-bench"]

  allow {
    protocol = "all"
  }
}

resource "google_compute_instance" "node" {
  for_each = local.roles

  name         = "craq-bench-${var.run_id}-${each.key}"
  machine_type = each.value.machine_type
  zone         = local.zones[each.value.subnet_index]
  tags         = ["craq-bench", replace(each.key, "_", "-")]

  labels = {
    app        = "craq-bench"
    managed_by = "terraform"
    run_id     = var.run_id
    role       = each.key
  }

  boot_disk {
    auto_delete = true

    initialize_params {
      image = data.google_compute_image.ubuntu_arm64.self_link
      size  = each.value.boot_disk_gib
      # C4A instances don't support pd-ssd boot disks.
      type = "hyperdisk-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.bench[each.value.subnet_index].id

    dynamic "access_config" {
      for_each = each.value.public_ip ? [1] : []
      content {}
    }
  }

  metadata = {
    ssh-keys = "${var.ssh_user}:${var.ssh_public_key}"
    startup-script = templatefile("${path.module}/user_data.sh.tftpl", {
      role           = each.value.role
      ssh_user       = var.ssh_user
      storage_layout = var.storage_layout
    })
  }
}
