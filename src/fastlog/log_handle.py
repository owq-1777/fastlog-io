from __future__ import annotations

import logging
import os
import queue
import re
import threading
import time
import urllib.error
import urllib.request
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass
from importlib import metadata
from typing import Callable, Any
from .config import Config

_QUEUE_BACKLOG_WARN = 1000
_QUEUE_SENTINEL = object()

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


def _group_pending_items(items: list[_Pending]) -> dict[str, dict[str, _TraceGroup]]:
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
    return by_family


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
        http_attempts: int = 3,
    ) -> None:
        self.endpoint = endpoint
        self.headers = dict(headers)  # defensive copy
        self.timeout = timeout
        self._retry = retry
        self._http_attempts = http_attempts

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

        self._retry(build_request, on_success, 'HTTP notification', self._http_attempts)


class _TelegramTransport:
    def __init__(
        self,
        telegram_url: str,
        chat_id: str,
        timeout: float,
        retry: Callable[[Callable[[], urllib.request.Request], Callable[[Any], None], str, int], None],
        http_attempts: int = 3,
    ) -> None:
        self.telegram_url = telegram_url
        self.chat_id = chat_id
        self.timeout = timeout
        self._retry = retry
        self._http_attempts = http_attempts

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

        self._retry(build_request, on_success, 'Telegram notification', self._http_attempts)


# -------------------------
# LogNotificationHandler
# -------------------------


class LogNotificationHandler(BaseLogHandler):
    """Batch log notifications, deduplicate, and deliver over HTTP, Telegram, and/or OTLP (HTTP or gRPC).

    Behavior:
      - Lines are buffered via `handle()`.
      - A "primary" line (>= min_level) triggers a sending window.
      - If `debounce=False` (default): first primary sets a fixed deadline (tumbling window).
      - If `debounce=True` : every primary pushes the deadline forward (debounce window).
      - All destinations share one aggregation window (`window_seconds`).
      - Repeated adjacent lines are coalesced using `_dedup_key()`.
      - `flush_expired()` lets external code drive deadline checks if new lines are infrequent.
      - OTLP and HTTP/Telegram delivery run in background worker threads; `flush()` waits for queued work to finish.
      - `close()` drains queues then stops workers (call from shutdown paths such as the directory watcher).
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
        window_seconds: float = 30.0,
        max_bytes: int = 4096,
        otlp_endpoint: str | None = None,
        otlp_service_name: str | None = None,
        otlp_headers: dict[str, str] | None = None,
        otlp_max_batch: int = 256,
        otlp_protocol: str | None = None,
        http_attempts: int = 3,
        *,
        debounce: bool = False,
    ) -> None:
        """Initialize notification delivery.

        Args:
            endpoint: Destination URL for log payloads. Optional if Telegram or OTLP is configured.
            telegram_token: Optional Telegram bot token for Telegram delivery.
            telegram_chat_id: Target Telegram chat ID used when sending messages.
            min_level: Minimum severity that triggers delivery (treats aliases via normalization).
            timeout: HTTP timeout in seconds for each POST.
            headers: Extra HTTP headers merged with defaults.
            config: Optional Config override for default levels.
            window_seconds: Aggregation window in seconds for all transports (OTLP, HTTP, Telegram).
            max_bytes: Maximum payload size in bytes; payload will be truncated to fit.
            otlp_endpoint: OTLP collector URL (HTTP: ``.../v1/logs`` or ``:4318``; gRPC: ``https://host``). Requires fastlog-io[otlp].
            otlp_service_name: service.name resource attribute (default: $OTEL_SERVICE_NAME or fastlog).
            otlp_headers: Optional OTLP request headers (else exporter reads $OTEL_EXPORTER_OTLP_HEADERS).
            otlp_max_batch: Max log records per OTLP export request.
            otlp_protocol: ``auto`` (default), ``http``, or ``grpc``; auto uses $FASTLOG_OTLP_PROTOCOL or heuristics.
            http_attempts: Max attempts per HTTP/Telegram POST (default 3; same as historical behavior).
            debounce: If True, extend the deadline on every primary line (debounce window).
        """
        has_plain = bool(endpoint)
        has_tg = bool(telegram_token and telegram_chat_id)
        has_otlp = bool(otlp_endpoint)
        if not has_plain and not has_tg and not has_otlp:
            raise ValueError('Provide --endpoint, or Telegram credentials, or --otlp-endpoint')
        if bool(telegram_token) != bool(telegram_chat_id):
            raise ValueError('telegram_token and telegram_chat_id must be provided together')

        self.timeout = timeout
        self._http_attempts = max(1, int(http_attempts))
        self.headers = {'Content-Type': 'text/plain; charset=utf-8'}
        if headers:
            # merge but don't mutate caller's dict
            self.headers.update(dict(headers))

        self.config = config or Config()
        resolved = _normalize_level((min_level or self.config.level))
        self.min_level = resolved
        self.min_weight = LEVEL_WEIGHTS.get(resolved, 0)

        self.max_bytes = max(512, int(max_bytes))
        self._debounce = debounce

        self._deadline: float | None = None
        self._window_seconds = max(0.1, float(window_seconds))

        try:
            self._package_version = metadata.version('fastlog-io')
        except metadata.PackageNotFoundError:
            self._package_version = '0.0.0'

        self._otlp_service_name = (otlp_service_name or os.getenv('OTEL_SERVICE_NAME') or 'fastlog').strip()
        self._otlp_max_batch = max(1, int(otlp_max_batch))

        self._otlp_exporter: Any | None = None
        if otlp_endpoint:
            from .otlp_export import otlp_dependencies_installed

            if not otlp_dependencies_installed():
                raise ImportError(
                    'OTLP export requires optional dependencies (opentelemetry-sdk, OTLP exporters). '
                    'Install with: pip install "fastlog-io[otlp]"'
                )
            from .otlp_export import create_otlp_log_exporter

            self._otlp_exporter = create_otlp_log_exporter(
                otlp_endpoint,
                headers=otlp_headers,
                timeout=timeout,
                protocol=otlp_protocol,
            )

        self._entries: list[_Pending] = []
        self._has_primary = False

        self._closed = False
        self._otlp_queue: queue.Queue | None = queue.Queue() if self._otlp_exporter is not None else None
        self._otlp_thread: threading.Thread | None = None
        self._notify_thread: threading.Thread | None = None

        # Build transports
        self._transports: list[Any] = []
        if endpoint:
            self._transports.append(
                _HttpTransport(endpoint, self.headers, self.timeout, self._send_with_retry, self._http_attempts)
            )
        if telegram_token and telegram_chat_id:
            telegram_url = f'https://api.telegram.org/bot{telegram_token}/sendMessage'
            self._transports.append(
                _TelegramTransport(telegram_url, telegram_chat_id, self.timeout, self._send_with_retry, self._http_attempts)
            )

        self._notify_queue: queue.Queue | None = queue.Queue() if self._transports else None

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
                self._deadline = now + self._window_seconds
            else:
                if self._deadline is None:
                    self._deadline = now + self._window_seconds
                elif now >= self._deadline:
                    self._flush()

    def flush(self) -> None:
        """Enqueue a snapshot of all buffered logs (ignore min_level), then wait until OTLP/notify queues drain."""
        if self._closed:
            return
        self._flush(force=True)
        self._join_queues()

    def close(self, wait: bool = True) -> None:
        """Optionally `flush()`, then stop background workers. Safe to call more than once."""
        if self._closed:
            return
        if wait:
            self.flush()
        self._closed = True
        if self._otlp_queue is not None and self._otlp_thread is not None:
            self._otlp_queue.put(_QUEUE_SENTINEL)
            self._otlp_thread.join()
        if self._notify_queue is not None and self._notify_thread is not None:
            self._notify_queue.put(_QUEUE_SENTINEL)
            self._notify_thread.join()

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

    def _flatten_pending_for_export(self, items: list[_Pending], *, include_all: bool) -> list[_Pending]:
        by_family = _group_pending_items(items)
        out: list[_Pending] = []
        for _family, traces in by_family.items():
            for _trace_id, group in traces.items():
                if not include_all and not group.has_primary:
                    continue
                out.extend(group.items)
        return out

    def _export_otlp(self, pendings: list[_Pending]) -> None:
        from opentelemetry.sdk._logs.export import LogRecordExportResult

        from .otlp_export import build_read_write_records

        assert self._otlp_exporter is not None
        records = build_read_write_records(
            pendings,
            service_name=self._otlp_service_name,
            package_version=self._package_version,
        )
        step = self._otlp_max_batch
        for i in range(0, len(records), step):
            chunk = records[i : i + step]
            result = self._otlp_exporter.export(chunk)
            if result is not LogRecordExportResult.SUCCESS:
                logger.warning('OTLP log export failed (batch starting at %s)', i)

    def _maybe_warn_backlog(self, q: queue.Queue, name: str) -> None:
        try:
            n = q.qsize()
        except NotImplementedError:
            return
        if n > _QUEUE_BACKLOG_WARN:
            logger.warning('fastlog delivery backlog (%s queue size=%s)', name, n)

    def _ensure_otlp_worker(self) -> None:
        if self._otlp_thread is not None:
            return
        assert self._otlp_queue is not None
        t = threading.Thread(target=self._otlp_worker_loop, name='fastlog-otlp', daemon=True)
        self._otlp_thread = t
        t.start()

    def _ensure_notify_worker(self) -> None:
        if self._notify_thread is not None:
            return
        assert self._notify_queue is not None
        t = threading.Thread(target=self._notify_worker_loop, name='fastlog-notify', daemon=True)
        self._notify_thread = t
        t.start()

    def _otlp_worker_loop(self) -> None:
        q = self._otlp_queue
        assert q is not None
        while True:
            task = q.get()
            try:
                if task is _QUEUE_SENTINEL:
                    break
                self._export_otlp(task)
            except Exception:
                logger.exception('OTLP export worker failed')
            finally:
                q.task_done()

    def _notify_worker_loop(self) -> None:
        q = self._notify_queue
        assert q is not None
        while True:
            task = q.get()
            try:
                if task is _QUEUE_SENTINEL:
                    break
                self._send(task)
            except Exception:
                logger.exception('HTTP/Telegram notify worker failed')
            finally:
                q.task_done()

    def _enqueue_otlp(self, pendings: list[_Pending]) -> None:
        if self._otlp_queue is None:
            return
        self._ensure_otlp_worker()
        self._maybe_warn_backlog(self._otlp_queue, 'otlp')
        self._otlp_queue.put(pendings)

    def _enqueue_notify(self, payload: bytes) -> None:
        if self._notify_queue is None:
            return
        self._ensure_notify_worker()
        self._maybe_warn_backlog(self._notify_queue, 'notify')
        self._notify_queue.put(payload)

    def _join_queues(self) -> None:
        if self._otlp_queue is not None:
            self._otlp_queue.join()
        if self._notify_queue is not None:
            self._notify_queue.join()

    def _flush_if_due(self, now: float) -> None:
        if not self._entries:
            return
        if not self._has_primary or self._deadline is None:
            return
        if now >= self._deadline:
            self._flush()

    def _flush(self, *, force: bool = False) -> None:
        if self._closed:
            return
        if not self._entries:
            return
        if not (self._has_primary or force):
            return

        pendings: list[_Pending] | None = None
        payload: bytes | None = None

        if self._otlp_exporter is not None:
            flat = self._flatten_pending_for_export(self._entries, include_all=force)
            if flat:
                pendings = flat

        if self._transports:
            payload = self._build_payload(self._entries, include_all=force)
            if not payload:
                payload = None

        self._clear_buffer()

        if pendings:
            self._enqueue_otlp(pendings)
        if payload is not None:
            self._enqueue_notify(payload)

    def _clear_buffer(self) -> None:
        self._entries.clear()
        self._deadline = None
        self._has_primary = False

    def _build_payload(self, items: list[_Pending], *, include_all: bool = False) -> bytes | None:
        """Build the text payload. When include_all=True, do not filter by primary."""
        by_family = _group_pending_items(items)

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
