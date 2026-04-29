"""Build OpenTelemetry log records from parsed watcher lines and export via OTLP (HTTP or gRPC)."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from importlib.util import find_spec
from typing import Any, Sequence

__all__ = [
    'otlp_dependencies_installed',
    'configured_otlp_endpoint',
    'resolve_otlp_protocol',
    'create_otlp_log_exporter',
    'level_to_severity',
    'entry_time_unix_ns',
    'build_read_write_records',
]


_OTLP_HTTP_EXPORTER_MODULE = 'opentelemetry.exporter.otlp.proto.http._log_exporter'
_OTLP_GRPC_EXPORTER_MODULE = 'opentelemetry.exporter.otlp.proto.grpc._log_exporter'
_OTLP_SDK_LOGS_MODULE = 'opentelemetry.sdk._logs'


def _module_available(module: str) -> bool:
    return find_spec(module) is not None


def _is_secure_grpc_endpoint(endpoint: str | None) -> bool:
    value = (endpoint or '').strip().lower()
    return not value.startswith('http://')


def _validate_file_env(env_name: str) -> None:
    raw = os.getenv(env_name)
    if raw is None:
        return
    path = raw.strip()
    if not path:
        raise ValueError(f'{env_name} is set but empty')
    if not os.path.isfile(path):
        raise ValueError(f'{env_name} points to a missing file: {path}')
    if not os.access(path, os.R_OK):
        raise ValueError(f'{env_name} is not readable: {path}')
    if os.path.getsize(path) <= 0:
        raise ValueError(f'{env_name} points to an empty file: {path}')


def _validate_grpc_tls_env(endpoint: str | None) -> None:
    if not _is_secure_grpc_endpoint(endpoint):
        return
    for env_name in (
        'OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE',
        'OTEL_EXPORTER_OTLP_CERTIFICATE',
        'OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE',
        'OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE',
        'OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY',
        'OTEL_EXPORTER_OTLP_CLIENT_KEY',
        'GRPC_DEFAULT_SSL_ROOTS_FILE_PATH',
        'SSL_CERT_FILE',
    ):
        _validate_file_env(env_name)


def configured_otlp_endpoint(endpoint: str | None = None) -> str | None:
    value = (endpoint or '').strip()
    if value:
        return value
    for env_name in ('OTEL_EXPORTER_OTLP_LOGS_ENDPOINT', 'OTEL_EXPORTER_OTLP_ENDPOINT', 'FASTLOG_OTLP_ENDPOINT'):
        env_value = (os.getenv(env_name) or '').strip()
        if env_value:
            return env_value
    return None


def _normalize_otlp_protocol(value: str | None) -> str | None:
    protocol = (value or '').strip().lower()
    if protocol in ('', 'auto'):
        return protocol or None
    if protocol == 'http/protobuf':
        return 'http'
    if protocol in ('http', 'grpc'):
        return protocol
    return None


def otlp_dependencies_installed(endpoint: str | None = None, protocol: str | None = None) -> bool:
    """Check whether the SDK and the exporter needed for the selected protocol are installed."""
    if not _module_available(_OTLP_SDK_LOGS_MODULE):
        return False
    if endpoint is None and protocol is None:
        return _module_available(_OTLP_HTTP_EXPORTER_MODULE) or _module_available(_OTLP_GRPC_EXPORTER_MODULE)

    try:
        resolved = resolve_otlp_protocol(endpoint or '', protocol)
    except ValueError:
        return False
    exporter_module = _OTLP_HTTP_EXPORTER_MODULE if resolved == 'http' else _OTLP_GRPC_EXPORTER_MODULE
    return _module_available(exporter_module)


def resolve_otlp_protocol(endpoint: str | None, protocol: str | None) -> str:
    """Choose ``http`` (OTLP/HTTP) or ``grpc`` (OTLP/gRPC).

    Precedence: explicit ``protocol`` (http/grpc) > ``$OTEL_EXPORTER_OTLP_LOGS_PROTOCOL`` > auto
    (``/v1/logs`` or port ``4318`` → HTTP, else gRPC).
    """
    p_cli = _normalize_otlp_protocol(protocol)
    if p_cli in ('grpc', 'http'):
        return p_cli
    env_p = _normalize_otlp_protocol(os.getenv('OTEL_EXPORTER_OTLP_LOGS_PROTOCOL'))
    if env_p in ('grpc', 'http'):
        return env_p
    env_p = _normalize_otlp_protocol(os.getenv('OTEL_EXPORTER_OTLP_PROTOCOL'))
    if env_p in ('grpc', 'http'):
        return env_p
    resolved_endpoint = configured_otlp_endpoint(endpoint) or ''
    if p_cli is None or p_cli == 'auto':
        if 'v1/logs' in resolved_endpoint or ':4318' in resolved_endpoint:
            return 'http'
        return 'grpc'
    raise ValueError(f'Invalid OTLP protocol: {protocol!r} (use auto, http, grpc, or http/protobuf)')


def create_otlp_log_exporter(
    endpoint: str | None,
    *,
    headers: dict[str, str] | None,
    timeout: float | None,
    protocol: str | None,
) -> Any:
    """Instantiate the OTLP log exporter for HTTP or gRPC."""
    proto = resolve_otlp_protocol(endpoint, protocol)
    if proto == 'grpc':
        _validate_grpc_tls_env(endpoint)
    if proto == 'http':
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        kwargs: dict[str, Any] = {}
        if endpoint is not None:
            kwargs['endpoint'] = endpoint
        if timeout is not None:
            kwargs['timeout'] = timeout
        if headers is not None:
            kwargs['headers'] = headers
        return OTLPLogExporter(**kwargs)
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter as GrpcOTLPLogExporter

    kwargs: dict[str, Any] = {}
    if endpoint is not None:
        kwargs['endpoint'] = endpoint
    if timeout is not None:
        kwargs['timeout'] = timeout
    if headers is not None:
        kwargs['headers'] = headers
    return GrpcOTLPLogExporter(**kwargs)


def level_to_severity(level: str | None):
    """Map fastlog normalized level to OTel severity number and text."""
    from opentelemetry._logs.severity import SeverityNumber

    key = (level or 'UNKNOWN').upper()
    table: dict[str, tuple[SeverityNumber, str]] = {
        'DEBUG': (SeverityNumber.DEBUG, 'DEBUG'),
        'INFO': (SeverityNumber.INFO, 'INFO'),
        'WARNING': (SeverityNumber.WARN, 'WARNING'),
        'ERROR': (SeverityNumber.ERROR, 'ERROR'),
        'CRITICAL': (SeverityNumber.FATAL, 'CRITICAL'),
        'UNKNOWN': (SeverityNumber.UNSPECIFIED, 'UNKNOWN'),
    }
    num, text = table.get(key, (SeverityNumber.INFO, key))
    return num, text


def entry_time_unix_ns(time_str: str | None) -> int:
    """Parse Loguru-style timestamp (UTC) to Unix nanoseconds; fallback to now."""
    if not time_str or not time_str.strip():
        return time.time_ns()
    s = time_str.strip()
    try:
        if '.' in s:
            main, frac = s.rsplit('.', 1)
            digits = ''.join(ch for ch in frac if ch.isdigit())
            if not digits:
                frac_padded = '000000'
            else:
                frac_padded = (digits + '000000')[:6]
            dt = datetime.strptime(f'{main}.{frac_padded}', '%Y-%m-%d %H:%M:%S.%f')
        else:
            dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    except ValueError:
        return time.time_ns()


def _http_action_attrs(action: str | None, message: str) -> dict[str, str | int | float]:
    if (action or '').strip() != 'http':
        return {}

    parts = message.split()
    if len(parts) != 6:
        return {}

    method, path, status_code, process_time, client_ip, country_code = parts
    if not method or not path or not status_code.isdigit() or not process_time.endswith('ms'):
        return {}

    duration_raw = process_time[:-2]
    try:
        duration_ms = float(duration_raw)
    except ValueError:
        return {}

    return {
        'http.request.method': method.upper(),
        'url.path': path,
        'http.response.status_code': int(status_code),
        'fastlog.process_time_ms': duration_ms,
        'client.address': client_ip,
        'geo.country.iso_code': country_code.upper(),
    }


def build_read_write_records(
    pendings: Sequence[object],
    *,
    service_name: str | None,
    package_version: str,
) -> list:
    """Convert pending log lines to SDK ReadWriteLogRecord list.

    Expects items like ``fastlog.log_handle._Pending`` (duck-typed: ``entry``, ``count``).
    """
    from opentelemetry._logs import LogRecord as APILogRecord
    from opentelemetry.sdk._logs import ReadWriteLogRecord
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.util.instrumentation import InstrumentationScope

    scope = InstrumentationScope(name='fastlog-io', version=package_version)
    out: list = []

    for p in pendings:
        entry = p.entry  # type: ignore[attr-defined]
        count = int(p.count)  # type: ignore[attr-defined]
        resolved_service_name = (entry.family or '').strip() or service_name
        resolved_environment = (os.getenv('RUN_ENV') or os.getenv('ENV') or '').strip()
        resolved_service_version = (os.getenv('VERSION') or '').strip()
        resource_attrs: dict[str, str] = {}
        if resolved_environment:
            resource_attrs['deployment.environment'] = resolved_environment
        if resolved_service_name:
            resource_attrs['service.name'] = resolved_service_name
        if service_name:
            resource_attrs['service.namespace'] = service_name
        if resolved_service_version:
            resource_attrs['service.version'] = resolved_service_version
        resource = Resource.create(resource_attrs)

        sev_num, sev_text = level_to_severity(entry.level)
        message = (entry.message or entry.raw or '').strip() or '-'
        body = ' | '.join(part for part in (entry.trace_id, entry.action, message) if part)

        attrs: dict[str, str | int | float] = {
            'fastlog.family': entry.family,
        }
        if entry.trace_id:
            attrs['fastlog.trace_id'] = entry.trace_id
        if entry.action:
            attrs['fastlog.action'] = entry.action
        attrs.update(_http_action_attrs(entry.action, message))
        for key, value in sorted((entry.attrs or {}).items()):
            attrs[f'fastlog.{key}'] = value
        if count > 1:
            attrs['log_count'] = count

        ts_ns = entry_time_unix_ns(entry.time)
        lr = APILogRecord(
            timestamp=ts_ns,
            severity_number=sev_num,
            severity_text=sev_text,
            body=body,
            attributes=attrs,
        )
        out.append(ReadWriteLogRecord(log_record=lr, resource=resource, instrumentation_scope=scope))

    return out
