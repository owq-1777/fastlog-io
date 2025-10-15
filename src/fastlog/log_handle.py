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

# -------------------------
# Constants & Logger
# -------------------------

# ANSI escape removal
ANSI_ESCAPE_RE = re.compile(r'\x1b\[[0-9;]*m')

# Only keep normalized level keys here.
LEVEL_WEIGHTS: dict[str, int] = {
    'DEBUG': 20,
    'INFO': 30,
    'WARNING': 40,
    'ERROR': 50,
    'CRITICAL': 60,
}

logger = logging.getLogger(__name__)


# -------------------------
# Utilities
# -------------------------


def strip_ansi(value: str) -> str:
    return ANSI_ESCAPE_RE.sub('', value)


def _normalize_level(level: str | None) -> str:
    """Normalize level aliases to standard keys used in LEVEL_WEIGHTS."""
    if not level:
        return 'UNKNOWN'
    up = level.upper()
    return {'WARN': 'WARNING', 'FATAL': 'CRITICAL', 'SUCCESS': 'INFO'}.get(up, up)


# -------------------------
# API Types
# -------------------------


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


class _TraceGroup:
    __slots__ = ('items', 'has_primary')

    def __init__(self) -> None:
        self.items: list[_Pending] = []
        self.has_primary = False


# -------------------------
# Transports (decoupled)
# -------------------------


class _HttpTransport:
    def __init__(
        self,
        endpoint: str,
        headers: dict[str, str],
        timeout: float,
        retry: Callable[[Callable[[], urllib.request.Request], Callable[[Any], None], str, int], None],
    ) -> None:
        self.endpoint = endpoint
        self.headers = dict(headers)  # defensive copy
        self.timeout = timeout
        self._retry = retry

    def send(self, payload: bytes) -> None:
        headers = dict(self.headers)
        headers.setdefault('User-Agent', 'LogNotifier/1.0')

        def build_request() -> urllib.request.Request:
            return urllib.request.Request(
                self.endpoint,
                data=payload,
                headers=headers,
                method='POST',
            )

        def on_success(response: Any) -> None:
            status = getattr(response, 'status', None)
            if status is not None:
                logger.info(f'HTTP notification delivered (status={status}, bytes={len(payload)})')
            else:
                logger.info(f'HTTP notification delivered (bytes={len(payload)})')

        self._retry(build_request, on_success, 'HTTP notification', 3)


class _TelegramTransport:
    def __init__(
        self,
        telegram_url: str,
        chat_id: str,
        timeout: float,
        retry: Callable[[Callable[[], urllib.request.Request], Callable[[Any], None], str, int], None],
    ) -> None:
        self.telegram_url = telegram_url
        self.chat_id = chat_id
        self.timeout = timeout
        self._retry = retry

    def send(self, payload: bytes) -> None:
        text = payload.decode('utf-8', 'replace')
        max_len = 4096
        if len(text) > max_len:
            text = text[: max_len - 1] + '…'
        body = {
            'chat_id': self.chat_id,
            'text': text,
            'disable_web_page_preview': 'true',
        }
        data = urllib.parse.urlencode(body).encode('utf-8')
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        def build_request() -> urllib.request.Request:
            return urllib.request.Request(
                self.telegram_url,
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

        self._retry(build_request, on_success, 'Telegram notification', 3)


# -------------------------
# LogNotificationHandler
# -------------------------


class LogNotificationHandler(BaseLogHandler):
    """Batch log notifications, deduplicate, and deliver over HTTP/Telegram.

    Behavior:
      - Lines are buffered via `handle()`.
      - A "primary" line (>= min_level) triggers a sending window.
      - If `debounce=False` (default): first primary sets a fixed deadline (tumbling window).
      - If `debounce=True` : every primary pushes the deadline forward (debounce window).
      - Repeated adjacent lines are coalesced using `_dedup_key()`.
      - `flush_expired()` lets external code drive deadline checks if new lines are infrequent.
      - `flush()` forces a send of ALL buffered lines (ignores min_level).
    """

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
        *,
        debounce: bool = False,
    ) -> None:
        """Initialize notification delivery.

        Args:
            endpoint: Destination URL for log payloads. Optional if Telegram is configured.
            telegram_token: Optional Telegram bot token for Telegram delivery.
            telegram_chat_id: Target Telegram chat ID used when sending messages.
            min_level: Minimum severity that triggers delivery (treats aliases via normalization).
            timeout: HTTP timeout in seconds for each POST.
            headers: Extra HTTP headers merged with defaults.
            config: Optional Config override for default levels.
            window_minutes: Delay window before sending accumulated logs.
            max_bytes: Maximum payload size in bytes; payload will be truncated to fit.
            debounce: If True, extend the deadline on every primary line (debounce window).
        """
        if not endpoint and not (telegram_token and telegram_chat_id):
            raise ValueError('Either endpoint or both telegram_token and telegram_chat_id must be provided')
        if bool(telegram_token) != bool(telegram_chat_id):
            raise ValueError('telegram_token and telegram_chat_id must be provided together')

        self.timeout = timeout
        self.headers = {'Content-Type': 'text/plain; charset=utf-8'}
        if headers:
            # merge but don't mutate caller's dict
            self.headers.update(dict(headers))

        self.config = config or Config()
        resolved = _normalize_level((min_level or self.config.level))
        self.min_level = resolved
        self.min_weight = LEVEL_WEIGHTS.get(resolved, 0)

        self.window_seconds = max(0.1, float(window_minutes) * 60.0)
        self.max_bytes = max(512, int(max_bytes))
        self._debounce = debounce

        # Buffer state
        self._entries: list[_Pending] = []
        self._deadline: float | None = None
        self._has_primary = False

        # Build transports
        self._transports: list[Any] = []
        if endpoint:
            self._transports.append(_HttpTransport(endpoint, self.headers, self.timeout, self._send_with_retry))
        if telegram_token and telegram_chat_id:
            telegram_url = f'https://api.telegram.org/bot{telegram_token}/sendMessage'
            self._transports.append(_TelegramTransport(telegram_url, telegram_chat_id, self.timeout, self._send_with_retry))

    # ---------- Public API ----------

    def handle(self, line: str, family: str) -> None:
        """Ingest one log line; may open/close a window and trigger sending."""
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
            if self._debounce:
                # Debounce: push the window every time a primary arrives
                self._deadline = now + self.window_seconds
            else:
                # Tumbling: only the first primary sets the deadline
                if self._deadline is None:
                    self._deadline = now + self.window_seconds
                elif now >= self._deadline:
                    self._flush()

    def flush(self) -> None:
        """Force sending ALL buffered logs immediately (ignore min_level)."""
        self._flush(force=True)

    def flush_expired(self) -> None:
        """Check the deadline against current time and flush if elapsed."""
        self._flush_if_due(time.monotonic())

    # ---------- Internal helpers ----------

    @staticmethod
    def _dedup_key(e: LogEntry) -> tuple[str, str, str, str, str]:
        """Key for adjacent duplicate folding."""
        return (
            e.family,
            e.trace_id or '',
            e.level or '',
            e.action or '',
            e.message or '',
        )

    @classmethod
    def _is_dup(cls, a: LogEntry, b: LogEntry) -> bool:
        return cls._dedup_key(a) == cls._dedup_key(b)

    def _flush_if_due(self, now: float) -> None:
        if not self._entries or not self._has_primary or self._deadline is None:
            return
        if now >= self._deadline:
            self._flush()

    def _flush(self, *, force: bool = False) -> None:
        if not self._entries:
            return
        if not (self._has_primary or force):
            return

        payload = self._build_payload(self._entries, include_all=force)
        if payload is None:
            self._clear_buffer()
            return

        self._send(payload)
        self._clear_buffer()

    def _clear_buffer(self) -> None:
        self._entries.clear()
        self._deadline = None
        self._has_primary = False

    def _build_payload(self, items: list[_Pending], *, include_all: bool = False) -> bytes | None:
        """Build the text payload. When include_all=True, do not filter by primary."""
        by_family: dict[str, dict[str, _TraceGroup]] = {}
        for pending in items:
            entry = pending.entry
            fam_groups = by_family.setdefault(entry.family, {})
            trace_id = entry.trace_id or 'trace:-'
            group = fam_groups.get(trace_id)
            if group is None:
                group = _TraceGroup()
                fam_groups[trace_id] = group
            group.items.append(pending)
            if pending.primary:
                group.has_primary = True

        lines: list[str] = []
        family_emitted = False
        for family, traces in by_family.items():
            emitted_this_family = False
            for trace_id, group in traces.items():
                if not include_all and not group.has_primary:
                    continue
                if not emitted_this_family:
                    if family_emitted:
                        lines.append('---')
                    lines.append(f'[{family}]')
                    emitted_this_family = True
                    family_emitted = True
                lines.append(f'\n{trace_id}')
                for pending in group.items:
                    entry = pending.entry
                    message = (entry.message or '-').replace('\n', '\\n')
                    level = entry.level or 'UNKNOWN'
                    action = entry.action or '-'
                    prefix = f'    x{pending.count} ' if pending.count > 1 else '    '
                    lines.extend((f'  {level} | {action}', f'{prefix}{message}'))

        if not lines:
            return None

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
        for t in self._transports:
            t.send(payload)

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
