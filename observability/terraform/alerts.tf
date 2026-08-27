locals {
  evaluation_interval = "30s"
}

resource "grafana_rule_group" "payments" {
  name             = "payments"
  folder_uid       = grafana_folder.payments.uid
  interval_seconds = 30

  rule {
    name      = "Ingestion has stopped"
    condition = "threshold"
    for       = "2m"

    annotations = {
      summary = "No change events reached bronze in the last ${var.heartbeat_window} while the consumer is up."
      runbook = "Injected by: make chaos-empty. Detects a source that stopped producing, not a consumer that died; that one shows as up == 0."
    }

    no_data_state  = "Alerting"
    exec_err_state = "Alerting"

    data {
      ref_id         = "query"
      datasource_uid = grafana_data_source.prometheus.uid
      relative_time_range {
        from = 900
        to   = 0
      }
      model = jsonencode({
        refId = "query"
        expr    = "sum(increase(cdc_records_written_total[${var.heartbeat_window}]))"
        instant = true
      })
    }

    data {
      ref_id         = "threshold"
      datasource_uid = "__expr__"
      relative_time_range {
        from = 0
        to   = 0
      }
      model = jsonencode({
        refId      = "threshold"
        type       = "threshold"
        expression = "query"
        conditions = [{
          evaluator = { type = "lt", params = [1] }
        }]
      })
    }
  }

  rule {
    name      = "Replication slot is retaining WAL"
    condition = "threshold"
    for       = "2m"

    annotations = {
      summary = "A replication slot is holding back more than ${var.retained_wal_bytes_threshold} bytes of WAL."
      runbook = "Injected by: make chaos-orphan-slot. Read straight from Postgres because a consumer that died cannot export a metric about itself."
    }

    no_data_state  = "Alerting"
    exec_err_state = "Alerting"

    data {
      ref_id         = "query"
      datasource_uid = grafana_data_source.payments_postgres.uid
      relative_time_range {
        from = 600
        to   = 0
      }
      model = jsonencode({
        refId      = "query"
        format     = "table"
        rawSql     = "SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), 0) AS retained_bytes FROM pg_replication_slots"
        rawQuery   = true
      })
    }

    data {
      ref_id         = "threshold"
      datasource_uid = "__expr__"
      relative_time_range {
        from = 0
        to   = 0
      }
      model = jsonencode({
        refId      = "threshold"
        type       = "threshold"
        expression = "query"
        conditions = [{
          evaluator = { type = "gt", params = [var.retained_wal_bytes_threshold] }
        }]
      })
    }
  }
}

resource "grafana_rule_group" "known_blind" {
  name             = "known-blind"
  folder_uid       = grafana_folder.payments.uid
  interval_seconds = 30

  rule {
    name      = "Archive ingestion stalled (KNOWN BLIND, see ADR 0006)"
    condition = "threshold"
    for       = "2m"

    annotations = {
      summary = "This alert is incapable of firing, on purpose. See ADR 0006."
      runbook = <<-EOT
        Kept deliberately, not a defect. It is blind for two reasons that are each
        defensible alone and fatal together, which is how alerts go blind in production.

        One: the query filters table="payments_archive", a label value no series carries.
        A table was renamed and the alert was not. The query returns nothing, forever.

        Two: no_data_state is OK. On its own that is a reasonable way to silence an
        intermittent series. Combined with a query that never returns data, it means the
        alert reports healthy forever.

        The vacuity guard in tests/ finds alerts blind this way. This one is its single
        declared exception, which is why it must stay.
      EOT
    }

    no_data_state  = "OK"
    exec_err_state = "OK"

    data {
      ref_id         = "query"
      datasource_uid = grafana_data_source.prometheus.uid
      relative_time_range {
        from = 900
        to   = 0
      }
      model = jsonencode({
        refId   = "query"
        expr    = "sum(increase(cdc_records_written_total{table=\"payments_archive\"}[10m]))"
        instant = true
      })
    }

    data {
      ref_id         = "threshold"
      datasource_uid = "__expr__"
      relative_time_range {
        from = 0
        to   = 0
      }
      model = jsonencode({
        refId      = "threshold"
        type       = "threshold"
        expression = "query"
        conditions = [{
          evaluator = { type = "lt", params = [1] }
        }]
      })
    }
  }
}
