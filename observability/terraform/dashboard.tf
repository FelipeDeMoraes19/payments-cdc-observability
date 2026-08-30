locals {
  postgres_plugin_id = "grafana-postgresql-datasource"

  dashboard = {
    title         = "payments pipeline"
    uid           = "payments-pipeline"
    schemaVersion = 39
    time          = { from = "now-1h", to = "now" }
    refresh       = "30s"
    panels = [
      {
        type    = "timeseries"
        title   = "Change events reaching bronze — flat means make chaos-empty"
        gridPos = { h = 8, w = 12, x = 0, y = 0 }
        datasource = { type = "prometheus", uid = grafana_data_source.prometheus.uid }
        targets = [{
          refId  = "A"
          expr   = "sum by (table) (rate(cdc_records_written_total[5m]))"
          legendFormat = "{{table}}"
        }]
      },
      {
        type    = "timeseries"
        title   = "Confirmed LSN — flat means the slot stopped advancing"
        gridPos = { h = 8, w = 12, x = 12, y = 0 }
        datasource = { type = "prometheus", uid = grafana_data_source.prometheus.uid }
        targets = [{
          refId = "A"
          expr  = "cdc_confirmed_lsn"
        }]
      },
      {
        type    = "table"
        title   = "Replication slots — retained WAL grows on make chaos-orphan-slot"
        gridPos = { h = 8, w = 12, x = 0, y = 8 }
        datasource = { type = local.postgres_plugin_id, uid = grafana_data_source.payments_postgres.uid }
        targets = [{
          refId    = "A"
          format   = "table"
          rawQuery = true
          rawSql   = "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal, confirmed_flush_lsn FROM pg_replication_slots ORDER BY slot_name"
        }]
      },
      {
        type    = "alertlist"
        title   = "Alert rules — one of them cannot fire, on purpose"
        gridPos = { h = 8, w = 12, x = 12, y = 8 }
        options = {
          viewMode                 = "list"
          groupMode                = "default"
          maxItems                 = 10
          sortOrder                = 1
          alertInstanceLabelFilter = ""
          showInstances            = false
          stateFilter = {
            firing   = true
            pending  = true
            noData   = true
            normal   = true
            error    = true
          }
        }
      },
    ]
  }
}

resource "grafana_dashboard" "payments" {
  folder      = grafana_folder.payments.uid
  config_json = jsonencode(local.dashboard)
}
