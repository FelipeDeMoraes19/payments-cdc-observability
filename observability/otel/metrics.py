import os

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from prometheus_client import start_http_server


def metrics_port() -> int:
    return int(os.environ.get("CDC_METRICS_PORT", "9108"))


def serve(service: str):
    reader = PrometheusMetricReader()
    metrics.set_meter_provider(
        MeterProvider(
            resource=Resource.create({SERVICE_NAME: service}),
            metric_readers=[reader],
        )
    )
    start_http_server(metrics_port())
    return metrics.get_meter(service)
