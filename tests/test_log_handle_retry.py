"""HTTP transport retries: real _send_with_retry path via patched urlopen."""

import urllib.error
from unittest.mock import MagicMock, patch

from fastlog.log_handle import LogNotificationHandler

_ERR = '2025-01-02 00:00:00.000 | ERROR    | tid | a | x | boom'


def _ok_response() -> MagicMock:
    m = MagicMock()
    m.__enter__.return_value = m
    m.__exit__.return_value = False
    m.status = 200
    return m


@patch('fastlog.log_handle.urllib.request.urlopen')
def test_http_retry_first_urlerror_second_success(mock_urlopen) -> None:
    mock_urlopen.side_effect = [urllib.error.URLError('conn'), _ok_response()]
    h = LogNotificationHandler(endpoint='http://127.0.0.1:9/x', timeout=0.01, http_attempts=3)
    try:
        h.handle(_ERR, 'fam')
        h.flush()
    finally:
        h.close()
    assert mock_urlopen.call_count == 2


@patch('fastlog.log_handle.urllib.request.urlopen')
def test_http_retry_exhausts_attempts(mock_urlopen) -> None:
    mock_urlopen.side_effect = urllib.error.URLError('fail')
    h = LogNotificationHandler(endpoint='http://127.0.0.1:9/x', timeout=0.01, http_attempts=2)
    try:
        h.handle(_ERR, 'fam')
        h.flush()
    finally:
        h.close()
    assert mock_urlopen.call_count == 2


@patch('fastlog.log_handle.urllib.request.urlopen')
def test_http_retry_single_attempt(mock_urlopen) -> None:
    mock_urlopen.side_effect = urllib.error.URLError('fail')
    h = LogNotificationHandler(endpoint='http://127.0.0.1:9/x', timeout=0.01, http_attempts=1)
    try:
        h.handle(_ERR, 'fam')
        h.flush()
    finally:
        h.close()
    assert mock_urlopen.call_count == 1
