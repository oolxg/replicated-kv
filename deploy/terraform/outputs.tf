output "router_public_ip" {
  value       = google_compute_instance.router.network_interface[0].access_config[0].nat_ip
  description = "Client-facing router address"
}

output "router_internal" {
  value       = "${google_compute_instance.router.network_interface[0].network_ip}:${var.kv_port}"
  description = "Router address as seen from the loadgen VM"
}

output "storage_internal_ips" {
  value = [for n in google_compute_instance.storage : n.network_interface[0].network_ip]
}

output "loadgen_ssh" {
  value       = var.loadgen_enabled ? "gcloud compute ssh kv-loadgen --zone ${var.zone}" : "(loadgen VM disabled)"
  description = "How to reach the k6 VM"
}

output "smoke_test" {
  value       = "curl -s -XPUT http://${google_compute_instance.router.network_interface[0].access_config[0].nat_ip}:${var.kv_port}/kv/hello -d '{\"value\":\"world\"}' && curl -s http://${google_compute_instance.router.network_interface[0].access_config[0].nat_ip}:${var.kv_port}/kv/hello"
  description = "Copy-paste smoke test against the deployed cluster"
}
