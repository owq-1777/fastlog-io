"""CLI behaviour."""

import io
from contextlib import redirect_stdout
from unittest.mock import patch

import pytest

from fastlog.cli import main


def test_main_no_argv_prints_help_and_returns_zero():
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = main([])
    assert code == 0
    out = buf.getvalue()
    assert 'usage: fastlog' in out
    assert 'Watcher:' in out
    assert 'Destinations:' in out
    assert 'Batching:' in out


def test_main_invalid_fastlog_http_attempts_exits(monkeypatch):
    monkeypatch.setenv('FASTLOG_HTTP_ATTEMPTS', 'bad')
    with pytest.raises(SystemExit):
        main(['/tmp', '--endpoint', 'http://x'])


def test_main_non_positive_window_seconds_exits():
    with pytest.raises(SystemExit):
        main(['/tmp', '--endpoint', 'http://x', '--window-seconds', '0'])


def test_main_accepts_native_otel_otlp_endpoint(monkeypatch):
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'https://collector.example.com')
    seen: dict[str, object] = {}

    class _Handler:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    class _Watcher:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def start(self):
            return None

    with patch('fastlog.cli.LogNotificationHandler', _Handler), patch('fastlog.cli.MultiLogWatcher', _Watcher):
        assert main(['/tmp']) == 0
    assert seen['otlp_endpoint'] is None
