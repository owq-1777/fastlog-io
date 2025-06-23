"""Prometheus metrics integration for logs package.

Expose counters to let Grafana/Prometheus track log volume by level and logger name.
"""
try:
    from prometheus_client import Counter, start_http_server

    LOG_COUNTER = Counter(
        'log_messages_total',
        'Total log messages',
        labelnames=('level', 'name'),
    )
except ImportError:  # pragma: no cover – optional dependency
    LOG_COUNTER = None
    Counter = None

__all__ = ['start_metrics_server', 'metrics_patch']


def metrics_patch(record: dict) -> None:
    '''Loguru patch: increment Prometheus counter for each record.'''
    if LOG_COUNTER is None:
        return
    level = record['level'].name.lower()
    name = record['extra'].get('name', '-')
    LOG_COUNTER.labels(level, name).inc()


def start_metrics_server(port: int = 8000):
    '''Start a background HTTP server on *port* exposing /metrics.'''
    if Counter is None:
        raise RuntimeError('prometheus_client is not installed')
    start_http_server(port)
