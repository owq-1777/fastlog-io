"""Async delivery: flush joins workers; worker failures must not deadlock flush."""

import time
from pathlib import Path

import pytest
from fastlog.log_handle import LogNotificationHandler

_ERR_LINE = '2025-01-02 00:00:00.000 | ERROR    | tid | a | x | boom'


def test_flush_blocks_until_notify_worker_completes():
    class _Slow(LogNotificationHandler):
        def _send(self, _payload: bytes) -> None:
            time.sleep(0.2)

    h = _Slow(endpoint='http://127.0.0.1:9/nope', timeout=0.01)
    h.handle(_ERR_LINE, 'fam')
    t0 = time.monotonic()
    h.flush()
    assert time.monotonic() - t0 >= 0.15
    h.close()


def test_notify_worker_exception_does_not_deadlock_flush():
    class _Flaky(LogNotificationHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self.calls = 0
            self.last_payload: bytes | None = None

        def _send(self, payload: bytes) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError('boom')
            self.last_payload = payload

    h = _Flaky(endpoint='http://127.0.0.1:9/nope', timeout=0.01)
    h.handle(_ERR_LINE, 'fam')
    h.flush()
    h.handle(_ERR_LINE, 'fam')
    t0 = time.monotonic()
    h.flush()
    assert time.monotonic() - t0 < 5.0
    assert h.calls == 2
    assert h.last_payload is not None
    h.close()


def test_close_shuts_down_otlp_exporter(monkeypatch):
    h = LogNotificationHandler(otlp_endpoint='http://127.0.0.1:4318/v1/logs')
    calls: list[bool] = []
    monkeypatch.setattr(h._otlp_exporter, 'shutdown', lambda: calls.append(True))
    h.close(wait=False)
    assert calls == [True]


def test_handler_uses_native_otel_otlp_endpoint(monkeypatch):
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://127.0.0.1:4318')
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_LOGS_PROTOCOL', 'http')
    h = LogNotificationHandler()
    try:
        assert h._otlp_exporter is not None
    finally:
        h.close(wait=False)


def test_handler_uses_otel_service_name_as_namespace_fallback(monkeypatch):
    monkeypatch.setenv('OTEL_SERVICE_NAME', 'checkout')
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://127.0.0.1:4318')
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_LOGS_PROTOCOL', 'http')
    h = LogNotificationHandler()
    try:
        assert h._otlp_service_name == 'checkout'
    finally:
        h.close(wait=False)


def test_handler_fails_fast_on_invalid_grpc_roots_file(monkeypatch, tmp_path: Path):
    roots = tmp_path / 'roots.pem'
    roots.write_text('', encoding='utf-8')
    monkeypatch.setenv('GRPC_DEFAULT_SSL_ROOTS_FILE_PATH', str(roots))
    with pytest.raises(ValueError, match='GRPC_DEFAULT_SSL_ROOTS_FILE_PATH'):
        LogNotificationHandler(otlp_endpoint='https://otel-grpc.aurtech.cc')
