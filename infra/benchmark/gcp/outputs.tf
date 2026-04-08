output "region" {
  value = var.gcp_region
}

output "primary_zone" {
  value = local.zones[0]
}

output "public_client_ip" {
  value = google_compute_instance.node["client"].network_interface[0].access_config[0].nat_ip
}

output "private_ips" {
  value = {
    for name, instance in google_compute_instance.node : name => instance.network_interface[0].network_ip
  }
}

output "instance_names" {
  value = {
    for name, instance in google_compute_instance.node : name => instance.name
  }
}

output "node_zones" {
  value = {
    for name, instance in google_compute_instance.node : name => instance.zone
  }
}
