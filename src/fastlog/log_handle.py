from __future__ import annotations

import logging
import re
import time
import urllib.error
import urllib.request
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Any
from .config import Config

__all__ = [
    'BaseLogHandler',
    'LogEntry',
    'parse_log_line',
    'LogNotificationHandler',
]

logger = logging.getLogger(__name__)

ANSI_ESCAPE_RE = re.compile(r'\x1b\[[0-9;]*m')
LEVEL_WEIGHTS: dict[str, int] = {
    'TRACE': 10,
    'DEBUG': 20,
    'INFO': 30,
    'SUCCESS': 35,
    'WARNING': 40,
    'WARN': 40,
    'ERROR': 50,
    'CRITICAL': 60,
    'FATAL': 60,
}


def strip_ansi(value: str) -> str:
    return ANSI_ESCAPE_RE.sub('', value)


def _normalize_level(level: str | None) -> str:
    if not level:
        return 'UNKNOWN'
    up = level.upper()
    if up == 'WARN':
        return 'WARNING'
    if up == 'FATAL':
        return 'CRITICAL'
    return up


class BaseLogHandler(ABC):
    @abstractmethod
    def handle(self, line: str, family: str) -> None: ...


@dataclass(slots=True)
class LogEntry:
    family: str
    raw: str
    time: str | None = None
    level: str | None = None
    trace_id: str | None = None
    action: str | None = None
    message: str | None = None


def parse_log_line(line: str, family: str) -> LogEntry:
    """Parse a loguru-formatted line into a structured entry."""
    cleaned = strip_ansi(line.rstrip('\n'))
    parts = cleaned.split(' | ', 4)

    entry = LogEntry(family=family, raw=line)
    if len(parts) == 5:
        entry.time = parts[0].strip() or None
        entry.level = parts[1].strip().upper() or None
        entry.trace_id = parts[2].strip() or None
        entry.action = parts[3].strip() or None
        entry.message = parts[4].strip() or None
    else:
        entry.message = cleaned.strip() or None
    return entry


@dataclass(slots=True)
class _Pending:
    entry: LogEntry
    count: int
    created_at: float
    primary: bool


class LogNotificationHandler(BaseLogHandler):
    """Batch log notifications, deduplicate, and deliver over HTTP."""

    def __init__(
        self,
        endpoint: str | None = None,
        telegram_token: str | None = None,
        telegram_chat_id: str | None = None,
        min_level: str | None = None,
        timeout: float = 5.0,
        headers: dict[str, str] | None = None,
        config: Config | None = None,
        window_minutes: float = 1.0,
        max_bytes: int = 4096,
    ) -> None:
        """Initialize notification delivery.

        Args:
            endpoint: Destination URL for log payloads. Optional if Telegram is configured.
            telegram_token: Optional Telegram bot token for Telegram delivery.
            telegram_chat_id: Target Telegram chat ID used when sending messages.
            min_level: Minimum severity that triggers delivery.
            timeout: HTTP timeout in seconds for each POST.
            headers: Extra HTTP headers merged with defaults.
            config: Optional Config override for default levels.
            window_minutes: Delay window before sending accumulated logs.
            max_bytes: Maximum payload size in bytes, enforcing truncation.
        """
        if not endpoint and not (telegram_token and telegram_chat_id):
            raise ValueError('Either endpoint or both telegram_token and telegram_chat_id must be provided')
        if bool(telegram_token) != bool(telegram_chat_id):
            raise ValueError('telegram_token and telegram_chat_id must be provided together')

        self.endpoint = endpoint
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self._telegram_url: str | None = f'https://api.telegram.org/bot{telegram_token}/sendMessage' if telegram_token else None
        self.timeout = timeout
        self.headers = {'Content-Type': 'text/plain; charset=utf-8'}
        if headers:
            self.headers.update(headers)

        self.config = config or Config()
        resolved = _normalize_level((min_level or self.config.level))
        self.min_level = resolved
        self.min_weight = LEVEL_WEIGHTS.get(resolved, 0)

        self.window_seconds = max(0.1, float(window_minutes) * 60.0)
        self.max_bytes = max(512, int(max_bytes))

        self._entries: list[_Pending] = []
        self._deadline: float | None = None
        self._has_primary = False

    def handle(self, line: str, family: str) -> None:
        now = time.monotonic()
        self._flush_if_due(now)

        e = parse_log_line(line, family)
        e.level = _normalize_level(e.level)
        weight = LEVEL_WEIGHTS.get(e.level, 0)
        is_primary = weight >= self.min_weight

        if self._entries and self._is_dup(e, self._entries[-1].entry):
            self._entries[-1].count += 1
            if is_primary:
                self._entries[-1].primary = True
        else:
            self._entries.append(_Pending(e, 1, now, is_primary))

        if is_primary:
            self._has_primary = True
            if self._deadline is None:
                self._deadline = now + self.window_seconds
            elif now >= self._deadline:
                self._flush(now)

    def flush(self) -> None:
        while self._entries:
            self._flush(time.monotonic(), force=True)

    def flush_expired(self) -> None:
        self._flush_if_due(time.monotonic())

    @staticmethod
    def _is_dup(a: LogEntry, b: LogEntry) -> bool:
        return (
            a.family == b.family
            and (a.level or '') == (b.level or '')
            and (a.action or '') == (b.action or '')
            and (a.message or '') == (b.message or '')
        )

    def _flush_if_due(self, now: float) -> None:
        if not self._entries or not self._has_primary or self._deadline is None:
            return
        if now >= self._deadline:
            self._flush(now)

    def _flush(self, now: float, *, force: bool = False) -> None:
        if not self._entries:
            return
        if not (self._has_primary or force):
            return

        payload = self._build_payload(self._entries)
        if payload is None:
            self._clear_buffer()
            return

        self._send(payload)
        self._clear_buffer()

    def _clear_buffer(self) -> None:
        self._entries.clear()
        self._deadline = None
        self._has_primary = False

    def _build_payload(self, items: list[_Pending]) -> bytes | None:
        by_family: dict[str, dict[str, list[_Pending]]] = {}
        for p in items:
            fam = p.entry.family
            tid = p.entry.trace_id or '(no-trace)'
            by_family.setdefault(fam, {}).setdefault(tid, []).append(p)

        families: list[tuple[str, dict[str, list[_Pending]]]] = []
        for fam, traces in by_family.items():
            filtered = {tid: ps for tid, ps in traces.items() if any(x.primary for x in ps)}
            if filtered:
                families.append((fam, filtered))

        if not families:
            return None

        lines: list[str] = []
        for i, (fam, traces) in enumerate(families):
            lines.append(f'[{fam}]')
            for tid, ps in traces.items():
                lines.append(tid)
                for p in ps:
                    msg = (p.entry.message or '-').replace('\n', '\\n')
                    lvl = p.entry.level or 'UNKNOWN'
                    action = p.entry.action or '-'
                    lines.append(f'  {lvl} | {action}')
                    prefix = f'    x{p.count} ' if p.count > 1 else '    '
                    lines.append(f'{prefix}{msg}')
            if i < len(families) - 1:
                lines.append('---')

        return self._shrink_and_encode(lines)

    def _shrink_and_encode(self, lines: list[str]) -> bytes | None:
        if not lines:
            return None
        working = list(lines)
        while working:
            data = '\n'.join(working).encode('utf-8')
            if len(data) <= self.max_bytes:
                return data
            last = working[-1]
            if len(last) > 4:
                working[-1] = last[: max(1, len(last) // 2)] + '…'
            else:
                working.pop()
        logger.warning('Payload truncated to meet max_bytes constraint (returning placeholder payload).')
        return b'[trimmed]'

    def _send(self, payload: bytes) -> None:
        if self.endpoint:
            self._send_http(payload)
        if self._telegram_url:
            self._send_telegram(payload)

    def _send_http(self, payload: bytes) -> None:
        def build_request() -> urllib.request.Request:
            return urllib.request.Request(
                self.endpoint,  # type: ignore[arg-type]
                data=payload,
                headers=self.headers,
                method='POST',
            )

        def on_success(response: Any) -> None:
            status = getattr(response, 'status', None)
            if status is not None:
                logger.info(f'HTTP notification delivered (status={status}, bytes={len(payload)})')
            else:
                logger.info(f'HTTP notification delivered (bytes={len(payload)})')

        self._send_with_retry(build_request, on_success, 'HTTP notification')

    def _send_telegram(self, payload: bytes) -> None:
        text = payload.decode('utf-8', 'replace')
        max_len = 4096
        if len(text) > max_len:
            text = text[: max_len - 1] + '…'
        body = {
            'chat_id': self.telegram_chat_id,
            'text': text,
            'disable_web_page_preview': True,
        }
        data = urllib.parse.urlencode(body).encode('utf-8')
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        def build_request() -> urllib.request.Request:
            return urllib.request.Request(
                self._telegram_url,  # type: ignore[arg-type]
                data=data,
                headers=headers,
                method='POST',
            )

        def on_success(response: Any) -> None:
            status = getattr(response, 'status', None)
            if status is not None:
                logger.info(f'Telegram notification delivered (status={status})')
            else:
                logger.info('Telegram notification delivered')

        self._send_with_retry(build_request, on_success, 'Telegram notification')

    def _send_with_retry(
        self,
        build_request: Callable[[], urllib.request.Request],
        on_success: Callable[[Any], None],
        context: str,
        attempts: int = 3,
    ) -> None:
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                with urllib.request.urlopen(build_request(), timeout=self.timeout) as resp:
                    on_success(resp)
                    return
            except urllib.error.URLError as exc:
                last_error = exc
                logger.warning(f'{context} failed (attempt {attempt}/{attempts}): {exc}')
            except Exception as exc:  # pragma: no cover
                logger.exception(f'Unexpected error during {context.lower()}: {exc}')
                return
        if last_error is not None:
            logger.error(f'{context} failed after {attempts} attempts: {last_error}')
        else:
            logger.error(f'{context} failed after {attempts} attempts.')
