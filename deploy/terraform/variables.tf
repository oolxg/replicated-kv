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

variable "machine_type" {
  description = "Machine type for storage nodes AND the router (identical across the 1/3/5 runs; override for the bonus scale-up experiment)"
  type        = string
  default     = "e2-small"
}

variable "loadgen_machine_type" {
  description = "Machine type for the k6 load generator — deliberately beefier so the generator is never the bottleneck"
  type        = string
  default     = "e2-standard-2"
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
