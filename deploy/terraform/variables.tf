variable "project" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  type    = string
  default = "europe-west3"
}

variable "zone" {
  type    = string
  default = "europe-west3-a"
}

variable "node_count" {
  description = "Number of storage nodes"
  type        = number
  default     = 3

  validation {
    condition     = var.node_count >= 1 && var.node_count <= 10
    error_message = "node_count must be between 1 and 10."
  }
}

# Storage nodes run on dedicated-core n1-standard-1 with the service
# cgroup-capped to storage_cpu_quota. Why not just a small shared-core VM:
# e2-micro/e2-small burst credits made benchmark numbers depend on the credit
# bucket, not the configuration (measured: same 3-node setup gave 7.3k and
# 11.7k req/s on different days). A dedicated core with a fixed CPU quota is
# deterministic; GCP simply sells nothing smaller than one dedicated vCPU, so
# the sub-core node is emulated in systemd instead.
variable "machine_type" {
  description = "Machine type for storage nodes (identical across the 1/3/5 runs; override for the bonus scale-up experiment)"
  type        = string
  default     = "n1-standard-1"
}

variable "storage_cpu_quota" {
  description = "systemd CPUQuota for the storage service (empty = uncapped). Emulates a sub-vCPU dedicated node for noise-free scaling benchmarks."
  type        = string
  default     = "25%"
}

# The router is deliberately much beefier than a storage node. A router
# request costs ~2.5x a storage request (same HTTP+JSON work PLUS a full
# HTTP-client hop to the replica), so an equal-hardware single coordinator
# caps the whole cluster below one node's capacity — measured: e2-small router
# 7.3k req/s, e2-standard-2 10k, vs 17.9k for one e2-small storage node hit
# directly. With quota-capped nodes (~0.25 vCPU) the 8 dedicated vCPUs here
# are ~32x a node — the coordinator can never hide the storage scaling curve.
variable "router_machine_type" {
  description = "Machine type for the router (see comment above)"
  type        = string
  default     = "e2-standard-8"
}

variable "loadgen_machine_type" {
  description = "Machine type for the k6 load generator — deliberately beefier so the generator is never the bottleneck"
  type        = string
  default     = "e2-standard-8"
}

variable "loadgen_enabled" {
  description = "Create the k6 loadgen VM (disable to drive load from outside the VPC when CPU quota is tight)"
  type        = bool
  default     = true
}

variable "kv_port" {
  type    = number
  default = 8080
}

variable "client_cidr" {
  description = "CIDR allowed to reach the router's client port and SSH (demo default: anywhere)"
  type        = string
  default     = "0.0.0.0/0"
}

# Quorum overrides. Empty string = let the application derive its defaults
# (RF = min(3, nodes), W = R = majority). Set kv_rf=1 for the pure-sharding
# throughput benchmark.
variable "kv_rf" {
  type    = string
  default = ""
}

variable "kv_w" {
  type    = string
  default = ""
}

variable "kv_r" {
  type    = string
  default = ""
}

# Load-shedding overrides; empty = application defaults.
variable "kv_shed_concurrent" {
  type    = string
  default = ""
}

variable "kv_shed_queue" {
  type    = string
  default = ""
}
