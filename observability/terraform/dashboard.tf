locals {
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
        type    = "timeseries"
        title   = "WAL retained by replication slots — grows on make chaos-orphan-slot"
        gridPos = { h = 8, w = 12, x = 0, y = 8 }
        datasource = { type = "postgres", uid = grafana_data_source.payments_postgres.uid }
        targets = [{
          refId    = "A"
          format   = "time_series"
          rawQuery = true
          rawSql   = "SELECT now() AS time, slot_name AS metric, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS value FROM pg_replication_slots"
        }]
      },
      {
        type    = "alertlist"
        title   = "Alert rules — one of them cannot fire, on purpose"
        gridPos = { h = 8, w = 12, x = 12, y = 8 }
        options = {
          viewMode       = "list"
          groupMode      = "default"
          maxItems       = 10
          sortOrder      = 1
          alertInstanceLabelFilter = ""
        }
      },
    ]
  }
}

resource "grafana_dashboard" "payments" {
  folder      = grafana_folder.payments.uid
  config_json = jsonencode(local.dashboard)
}
