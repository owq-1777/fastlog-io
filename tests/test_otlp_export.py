import time

import pytest

from fastlog.log_handle import LogEntry, _Pending, parse_log_line
from fastlog.otlp_export import (
    build_read_write_records,
    entry_time_unix_ns,
    level_to_severity,
    otlp_dependencies_installed,
    resolve_otlp_protocol,
)


def test_resolve_auto_http_when_v1_logs_in_path():
    assert resolve_otlp_protocol('https://collector/v1/logs', None) == 'http'


def test_resolve_auto_http_when_port_4318():
    assert resolve_otlp_protocol('http://127.0.0.1:4318/', None) == 'http'


def test_resolve_auto_grpc_for_https_host_without_4318():
    assert resolve_otlp_protocol('https://example.com', None) == 'grpc'


def test_resolve_explicit_protocol_overrides_url_shape():
    assert resolve_otlp_protocol('http://127.0.0.1:4318/v1/logs', 'grpc') == 'grpc'
    assert resolve_otlp_protocol('https://example.com', 'http') == 'http'


def test_resolve_env_used_when_protocol_none(monkeypatch):
    monkeypatch.setenv('FASTLOG_OTLP_PROTOCOL', 'http')
    assert resolve_otlp_protocol('https://grpc-only.example.com', None) == 'http'


def test_otlp_dependencies_installed_requires_only_selected_http_exporter(monkeypatch):
    availability = {
        'opentelemetry.sdk._logs': True,
        'opentelemetry.exporter.otlp.proto.http._log_exporter': True,
        'opentelemetry.exporter.otlp.proto.grpc._log_exporter': False,
    }
    monkeypatch.setattr('fastlog.otlp_export._module_available', availability.__getitem__)
    assert otlp_dependencies_installed('http://127.0.0.1:4318/v1/logs', None) is True


def test_otlp_dependencies_installed_without_protocol_accepts_any_exporter(monkeypatch):
    availability = {
        'opentelemetry.sdk._logs': True,
        'opentelemetry.exporter.otlp.proto.http._log_exporter': False,
        'opentelemetry.exporter.otlp.proto.grpc._log_exporter': True,
    }
    monkeypatch.setattr('fastlog.otlp_export._module_available', availability.__getitem__)
    assert otlp_dependencies_installed() is True


@pytest.mark.skipif(not otlp_dependencies_installed(), reason='fastlog-io[otlp] not installed')
class TestOtlpExportRecords:
    def test_level_to_severity_maps_known_levels(self):
        from opentelemetry._logs.severity import SeverityNumber

        n, t = level_to_severity('ERROR')
        assert n == SeverityNumber.ERROR
        assert t == 'ERROR'

    def test_entry_time_unix_ns_positive(self):
        ns = entry_time_unix_ns('2025-01-02 03:04:05.123')
        assert ns > 0

    def test_build_read_write_records_basic(self):
        e = LogEntry(
            family='app',
            raw='line',
            time='2025-01-02 03:04:05.000',
            level='ERROR',
            trace_id='tid',
            action='mod.fn:1',
            message='boom',
        )
        p = _Pending(e, 2, time.monotonic(), True)
        rows = build_read_write_records([p], service_name='svc', package_version='9.9.9')
        assert len(rows) == 1
        body = rows[0].log_record.body
        assert 'boom' in str(body)
        assert '(x2)' in str(body)
        attrs = dict(rows[0].log_record.attributes or {})
        assert attrs.get('fastlog.trace_id') == 'tid'

    def test_build_read_write_records_trace_id_from_parse_line(self):
        line = '2025-01-02 00:00:00.000 | INFO     | abcTraceId | mod.x:1 | hello'
        entry = parse_log_line(line, 'fam')
        p = _Pending(entry, 1, time.monotonic(), True)
        rows = build_read_write_records([p], service_name='svc', package_version='1.0.0')
        attrs = dict(rows[0].log_record.attributes or {})
        assert attrs['fastlog.trace_id'] == 'abcTraceId'
