"""Tumbling / debounce windows driven by flush_expired(); time.monotonic is fixed (no sleep)."""

from unittest.mock import MagicMock, patch

from fastlog.log_handle import LogNotificationHandler

_ERR = '2025-01-02 00:00:00.000 | ERROR    | tid | a | x | boom'


class _CountNotify(LogNotificationHandler):
    def __init__(self, *a, **kw) -> None:
        super().__init__(*a, **kw)
        self.notify_enqueues = 0

    def _enqueue_notify(self, payload: bytes) -> None:
        self.notify_enqueues += 1
        super()._enqueue_notify(payload)


def _patch_urlopen_ok(mock: MagicMock) -> None:
    m = MagicMock()
    m.__enter__.return_value = m
    m.__exit__.return_value = False
    m.status = 200
    mock.return_value = m


@patch('fastlog.log_handle.urllib.request.urlopen')
def test_tumbling_window_flush_expired_enqueues_after_deadline(mock_urlopen, monkeypatch) -> None:
    _patch_urlopen_ok(mock_urlopen)
    t0, t1, t2 = 1000.0, 1010.0, 1030.0
    seq = iter([t0, t1, t2])
    monkeypatch.setattr('fastlog.log_handle.time.monotonic', lambda: next(seq))

    h = _CountNotify(endpoint='http://example.com/n', timeout=0.01, window_seconds=30.0, debounce=False)
    try:
        h.handle(_ERR, 'fam')
        assert h.notify_enqueues == 0
        h.handle(_ERR, 'fam')
        assert h.notify_enqueues == 0
        h.flush_expired()
        assert h.notify_enqueues == 1
    finally:
        h.close()


@patch('fastlog.log_handle.urllib.request.urlopen')
def test_debounce_extends_deadline_second_primary(mock_urlopen, monkeypatch) -> None:
    _patch_urlopen_ok(mock_urlopen)
    t0, t1, t2, t3 = 1000.0, 1010.0, 1030.0, 1040.0
    seq = iter([t0, t1, t2, t3])
    monkeypatch.setattr('fastlog.log_handle.time.monotonic', lambda: next(seq))

    h = _CountNotify(endpoint='http://example.com/n', timeout=0.01, window_seconds=30.0, debounce=True)
    try:
        h.handle(_ERR, 'fam')
        h.handle(_ERR, 'fam')
        h.flush_expired()
        assert h.notify_enqueues == 0
        h.flush_expired()
        assert h.notify_enqueues == 1
    finally:
        h.close()
