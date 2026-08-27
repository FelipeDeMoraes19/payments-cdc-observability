terraform {
  required_version = ">= 1.6"
  required_providers {
    grafana = {
      source  = "grafana/grafana"
      version = "4.45.2"
    }
  }
}

provider "grafana" {
  url  = var.grafana_url
  auth = "${var.grafana_admin_user}:${var.grafana_admin_password}"
}

resource "grafana_data_source" "prometheus" {
  type       = "prometheus"
  name       = "prometheus"
  url        = var.prometheus_url
  is_default = true
}

resource "grafana_data_source" "payments_postgres" {
  type          = "postgres"
  name          = "payments-postgres"
  url           = var.postgres_url
  username      = var.postgres_user
  database_name = var.postgres_database

  secure_json_data_encoded = jsonencode({
    password = var.postgres_password
  })

  json_data_encoded = jsonencode({
    sslmode         = "disable"
    postgresVersion = 1600
  })
}

resource "grafana_folder" "payments" {
  title = "payments"
}
