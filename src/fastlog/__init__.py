"""logs package – import‑time ready logger with Prometheus metrics helper.

Public API:
    configure            – reconfigure sinks, level, etc.
    get_log(name=None)   – get bound logger
    log                  – the global loguru log
    start_metrics_server – start a Prometheus /metrics endpoint
"""
from .core import configure, get_log
from .metrics import start_metrics_server

__all__ = ['configure', 'get_log', 'log', 'start_metrics_server']

log = get_log('app')
