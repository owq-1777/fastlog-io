# FastLOG

FastLOG is a lightweight wrapper around [Loguru](https://github.com/Delgan/loguru) with sensible defaults, automatic `trace_id`, stdlib logging interception, and a directory watcher that can forward log bursts to HTTP, Telegram, or OTLP.

## Features

- Zero-config logging: `from fastlog import log; log.info("hello")`
- Automatic `trace_id` injection for application logs
- Simple file logging with rotation and retention via `configure()`
- Standard `logging` records are routed through FastLOG
- `fastlog` CLI tails `*.log` files and forwards high-severity events

## Installation

```bash
uv add fastlog-io
uv add "fastlog-io[otlp]"
```

Or with `pip`:

```bash
pip install fastlog-io
pip install "fastlog-io[otlp]"
```

Requirements:

- Python 3.12+
- [loguru](https://pypi.org/project/loguru/)

## Quickstart

```python
from fastlog import configure, log

configure(
    level="INFO",
    log_path="./logs/app.log",
    rotation="10 MB",
    retention="7 days",
)

log.info("service started")

with log.trace_ctx():
    log.bind(action="api.call").info("request done")

# or use
import logging
logger = logging.getLogger("my-app")
logger.warning("cache miss")
```

Example output:

```text
2025-10-13 12:23:00.193 | INFO     | 3hAhb3OFpU7zXKWBwp | ts.<module>:13 | service started
2025-10-13 12:23:00.194 | WARNING  | 1F1FwLwm4Orok0gs0a | [my-app]ts.<module>:16 | cache miss
2025-10-13 12:23:00.194 | INFO     | WG8yiDxzO3MCmWy8CB | api.call     | request done
```

## Watcher CLI

The `fastlog` CLI watches a directory of `*.log` files, resumes from stored byte offsets, batches matching lines, and sends them to one or more destinations.

```bash
uv tool install fastlog-io

# HTTP
uv run fastlog ./logs --endpoint "$FASTLOG_NOTIFY_ENDPOINT"

# Telegram
uv run fastlog ./logs \
  --tg-token "$FASTLOG_NOTIFY_TG_TOKEN" \
  --tg-chat-id "$FASTLOG_NOTIFY_TG_CHAT_ID"

# OTLP
uv run fastlog ./logs --otlp-endpoint "http://127.0.0.1:4318/v1/logs"
```

Key points:

- `--min-level` defaults to `WARNING`
- offsets are stored in `.multilogwatch.state.json`
- HTTP and Telegram retries default to `3`
- `flush()` drains queued batches; `close()` drains and stops workers
- OTLP uses the native OpenTelemetry exporter configuration model when possible

Run `uv run fastlog --help` for the full option list.

## OTLP Notes

Install `fastlog-io[otlp]` to enable OTLP export.

- `--otlp-endpoint` explicitly overrides the collector endpoint
- when `--otlp-endpoint` is unset, FastLOG falls back to `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`, then `OTEL_EXPORTER_OTLP_ENDPOINT`
- `--otlp-protocol`, `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL`, or `OTEL_EXPORTER_OTLP_PROTOCOL` selects `http`, `grpc`, or `http/protobuf`
- headers, timeout, TLS, and resource attributes are delegated to the native OpenTelemetry SDK/exporter env handling

Common OTLP env vars:

| Variable | Description |
| --- | --- |
| `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` | Logs-specific OTLP endpoint |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Generic OTLP endpoint fallback |
| `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL` | `http`, `grpc`, or `http/protobuf` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | Generic protocol fallback |
| `OTEL_EXPORTER_OTLP_LOGS_HEADERS` | Logs-specific request headers |
| `OTEL_EXPORTER_OTLP_LOGS_TIMEOUT` | Logs-specific exporter timeout |
| `OTEL_SERVICE_NAME` | Fallback source for resource `service.namespace` |
| `OTEL_RESOURCE_ATTRIBUTES` | Extra resource attributes |
| `RUN_ENV` / `ENV` | Resource `deployment.environment` when set |
| `VERSION` | Resource `service.version` when set |

## Common Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `LOG_PATH` | empty | File output path |
| `LOG_LEVEL` | `INFO` | Default log level |
| `LOG_ROTATION` | `100 MB` | Loguru rotation policy |
| `FASTLOG_NOTIFY_ENDPOINT` | empty | HTTP destination for watcher notifications |
| `FASTLOG_NOTIFY_LEVEL` | `WARNING` | Default watcher threshold |
| `FASTLOG_NOTIFY_TIMEOUT` | empty | HTTP timeout override in seconds |
| `FASTLOG_HTTP_ATTEMPTS` | empty | HTTP and Telegram retry count, default `3` |
| `FASTLOG_WINDOW_SECONDS` | `30` | Batch window in seconds |
| `FASTLOG_NOTIFY_MAX_BYTES` | `4096` | Max payload size per request |
| `FASTLOG_NOTIFY_TG_TOKEN` | empty | Telegram bot token |
| `FASTLOG_NOTIFY_TG_CHAT_ID` | empty | Telegram chat id |
| `FASTLOG_OTLP_MAX_BATCH` | `256` | Max records per OTLP export |

## Development

```bash
uv run --with '.[test]' pytest -q
uv build
uv run --with dist/*.whl --with pytest pytest -q
```

## License

[MIT](LICENSE) © 2025 OWQ
