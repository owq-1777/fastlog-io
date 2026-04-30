import time
from pathlib import Path

import pytest

from fastlog.log_handle import LogEntry, _Pending, parse_log_line
from fastlog.otlp_export import (
    build_read_write_records,
    configured_otlp_endpoint,
    create_otlp_log_exporter,
    entry_time_unix_ns,
    level_to_severity,
    otlp_dependencies_installed,
    resolve_otlp_protocol,
)


@pytest.fixture(autouse=True)
def _isolate_otlp_env(monkeypatch):
    for name in (
        'FASTLOG_OTLP_ENDPOINT',
        'GRPC_DEFAULT_SSL_ROOTS_FILE_PATH',
        'OTEL_EXPORTER_OTLP_CERTIFICATE',
        'OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE',
        'OTEL_EXPORTER_OTLP_CLIENT_KEY',
        'OTEL_EXPORTER_OTLP_ENDPOINT',
        'OTEL_EXPORTER_OTLP_HEADERS',
        'OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE',
        'OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE',
        'OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY',
        'OTEL_EXPORTER_OTLP_LOGS_ENDPOINT',
        'OTEL_EXPORTER_OTLP_LOGS_HEADERS',
        'OTEL_EXPORTER_OTLP_LOGS_PROTOCOL',
        'OTEL_EXPORTER_OTLP_LOGS_TIMEOUT',
        'OTEL_EXPORTER_OTLP_PROTOCOL',
        'OTEL_EXPORTER_OTLP_TIMEOUT',
        'OTEL_RESOURCE_ATTRIBUTES',
        'SSL_CERT_FILE',
    ):
        monkeypatch.delenv(name, raising=False)


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
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_LOGS_PROTOCOL', 'http')
    assert resolve_otlp_protocol('https://grpc-only.example.com', None) == 'http'


def test_resolve_generic_env_used_when_logs_protocol_missing(monkeypatch):
    monkeypatch.delenv('OTEL_EXPORTER_OTLP_LOGS_PROTOCOL', raising=False)
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_PROTOCOL', 'http/protobuf')
    assert resolve_otlp_protocol('https://grpc-only.example.com', None) == 'http'


def test_resolve_accepts_http_protobuf_logs_protocol(monkeypatch):
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_LOGS_PROTOCOL', 'http/protobuf')
    assert resolve_otlp_protocol('https://grpc-only.example.com', None) == 'http'


def test_configured_otlp_endpoint_prefers_logs_endpoint(monkeypatch):
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'https://collector.example.com')
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_LOGS_ENDPOINT', 'https://collector.example.com/v1/logs')
    assert configured_otlp_endpoint(None) == 'https://collector.example.com/v1/logs'


def test_resolve_protocol_uses_native_env_endpoint(monkeypatch):
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_LOGS_ENDPOINT', 'http://127.0.0.1:4318/v1/logs')
    assert resolve_otlp_protocol(None, None) == 'http'


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
    def test_create_otlp_log_exporter_rejects_empty_grpc_roots_file(self, monkeypatch, tmp_path: Path):
        roots = tmp_path / 'roots.pem'
        roots.write_text('', encoding='utf-8')
        monkeypatch.setenv('GRPC_DEFAULT_SSL_ROOTS_FILE_PATH', str(roots))
        with pytest.raises(ValueError, match='GRPC_DEFAULT_SSL_ROOTS_FILE_PATH'):
            create_otlp_log_exporter(
                'https://otel-grpc.aurtech.cc',
                headers=None,
                timeout=1.0,
                protocol='grpc',
            )

    def test_create_otlp_log_exporter_uses_native_http_env_headers(self, monkeypatch):
        monkeypatch.setenv('OTEL_EXPORTER_OTLP_HEADERS', 'Authorization=Bearer token,X-Scope-OrgID=tenant-a')
        exporter = create_otlp_log_exporter(
            'http://127.0.0.1:4318/v1/logs',
            headers=None,
            timeout=1.0,
            protocol='http',
        )
        assert exporter._headers == {'authorization': 'Bearer token', 'x-scope-orgid': 'tenant-a'}

    def test_create_otlp_log_exporter_uses_native_http_env_timeout(self, monkeypatch):
        monkeypatch.setenv('OTEL_EXPORTER_OTLP_TIMEOUT', '7.5')
        exporter = create_otlp_log_exporter(
            'http://127.0.0.1:4318/v1/logs',
            headers=None,
            timeout=None,
            protocol='http',
        )
        assert exporter._timeout == 7.5

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
        assert str(body) == 'boom'
        attrs = dict(rows[0].log_record.attributes or {})
        resource_attrs = dict(rows[0].resource.attributes or {})
        assert attrs.get('log_count') == 2
        assert attrs.get('fastlog.trace_id') == 'tid'
        assert resource_attrs['service.name'] == 'app'
        assert resource_attrs['service.namespace'] == 'svc'
        assert 'deployment.environment' not in resource_attrs

    def test_build_read_write_records_trace_id_from_parse_line(self):
        line = '2025-01-02 00:00:00.000 | INFO     | abcTraceId | mod.x:1 | hello'
        entry = parse_log_line(line, 'fam')
        p = _Pending(entry, 1, time.monotonic(), True)
        rows = build_read_write_records([p], service_name='svc', package_version='1.0.0')
        attrs = dict(rows[0].log_record.attributes or {})
        resource_attrs = dict(rows[0].resource.attributes or {})
        assert attrs['fastlog.trace_id'] == 'abcTraceId'
        assert resource_attrs['service.name'] == 'fam'
        assert resource_attrs['service.namespace'] == 'svc'

    def test_build_read_write_records_includes_structured_extra_attributes(self):
        line = '2025-01-02 00:00:00.000 | INFO     | abcTraceId | mod.x:1 | {"chat_id":"ops-room","send_msg":1} | hello'
        entry = parse_log_line(line, 'fam')
        p = _Pending(entry, 1, time.monotonic(), True)
        rows = build_read_write_records([p], service_name='svc', package_version='1.0.0')
        attrs = dict(rows[0].log_record.attributes or {})
        assert attrs['fastlog.send_msg'] == '1'
        assert attrs['fastlog.chat_id'] == 'ops-room'

    def test_build_read_write_records_preserves_pipe_inside_structured_extra_and_message(self):
        line = '2025-01-02 00:00:00.000 | INFO     | abcTraceId | mod.x:1 | {"note":"a | b"} | hello | world'
        entry = parse_log_line(line, 'fam')
        p = _Pending(entry, 1, time.monotonic(), True)
        rows = build_read_write_records([p], service_name='svc', package_version='1.0.0')
        attrs = dict(rows[0].log_record.attributes or {})
        assert attrs['fastlog.note'] == 'a | b'
        assert str(rows[0].log_record.body) == 'hello | world'

    def test_build_read_write_records_maps_http_action_message_attributes(self):
        e = LogEntry(
            family='app',
            raw='line',
            time='2025-01-02 03:04:05.000',
            level='INFO',
            trace_id='tid',
            action='http',
            message='GET /api/users 200 12.34ms 203.0.113.8 us',
        )
        p = _Pending(e, 1, time.monotonic(), True)
        rows = build_read_write_records([p], service_name='svc', package_version='1.0.0')
        attrs = dict(rows[0].log_record.attributes or {})
        assert attrs['http.request.method'] == 'GET'
        assert attrs['url.path'] == '/api/users'
        assert attrs['http.response.status_code'] == 200
        assert attrs['fastlog.process_time_ms'] == 12.34
        assert attrs['client.address'] == '203.0.113.8'
        assert attrs['geo.country.iso_code'] == 'US'
        assert str(rows[0].log_record.body) == 'GET /api/users 200 12.34ms 203.0.113.8 us'

    def test_build_read_write_records_ignores_malformed_http_action_message_attributes(self):
        e = LogEntry(
            family='app',
            raw='line',
            time='2025-01-02 03:04:05.000',
            level='INFO',
            action='http',
            message='GET /api/users 200',
        )
        p = _Pending(e, 1, time.monotonic(), True)
        rows = build_read_write_records([p], service_name='svc', package_version='1.0.0')
        attrs = dict(rows[0].log_record.attributes or {})
        assert attrs['fastlog.action'] == 'http'
        assert 'http.request.method' not in attrs
        assert 'http.response.status_code' not in attrs

    def test_build_read_write_records_uses_run_env_and_native_resource_attributes(self, monkeypatch):
        monkeypatch.setenv('RUN_ENV', 'prod')
        monkeypatch.setenv('VERSION', '1.2.3')
        monkeypatch.setenv('OTEL_RESOURCE_ATTRIBUTES', 'cloud.region=us-east-1')
        e = LogEntry(
            family='app',
            raw='line',
            time='2025-01-02 03:04:05.000',
            level='ERROR',
            message='boom',
        )
        p = _Pending(e, 1, time.monotonic(), True)
        rows = build_read_write_records([p], service_name=None, package_version='9.9.9')
        attrs = dict(rows[0].resource.attributes or {})
        assert attrs['service.name'] == 'app'
        assert attrs['deployment.environment'] == 'prod'
        assert attrs['service.version'] == '1.2.3'
        assert attrs['cloud.region'] == 'us-east-1'

    def test_build_read_write_records_service_name_falls_back_when_family_missing(self, monkeypatch):
        monkeypatch.setenv('ENV', 'staging')
        e = LogEntry(
            family='',
            raw='line',
            time='2025-01-02 03:04:05.000',
            level='ERROR',
            message='boom',
        )
        p = _Pending(e, 1, time.monotonic(), True)
        rows = build_read_write_records([p], service_name='svc-fallback', package_version='9.9.9')
        attrs = dict(rows[0].resource.attributes or {})
        assert attrs['service.name'] == 'svc-fallback'
        assert attrs['service.namespace'] == 'svc-fallback'
        assert attrs['deployment.environment'] == 'staging'
        assert 'service.version' not in attrs
