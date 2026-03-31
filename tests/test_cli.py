"""CLI behaviour."""

import io
from contextlib import redirect_stdout

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
