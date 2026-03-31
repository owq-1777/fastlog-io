"""Async delivery: flush joins workers; worker failures must not deadlock flush."""

import time

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
