variable "grafana_url" {
  type    = string
  default = "http://grafana:3000"
}

variable "grafana_admin_user" {
  type    = string
  default = "admin"
}

variable "grafana_admin_password" {
  type      = string
  sensitive = true
}

variable "prometheus_url" {
  type    = string
  default = "http://prometheus:9090"
}

variable "postgres_url" {
  type    = string
  default = "postgres:5432"
}

variable "postgres_user" {
  type    = string
  default = "grafana"
}

variable "postgres_database" {
  type    = string
  default = "payments"
}

variable "postgres_password" {
  type      = string
  sensitive = true
}

variable "heartbeat_window" {
  type    = string
  default = "10m"
}

variable "retained_wal_bytes_threshold" {
  type    = number
  default = 52428800
}
