<p align="center">
  <br>
  <a href="https://github.com/owq-1777/fastlog-io">
  	<img src="https://github.com/owq-1777/fastlog-io/raw/main/assets/fastlog.png" alt="FastLOG">
  </a>
</p>

<p align="center">
    <em>FastLOG is a lightweight wrapper around <a href="https://github.com/Delgan/loguru" target="_blank">Loguru</a> that offers Prometheus metrics, automatic `trace_id`, colourful output, and a dead‑simple configuration API.</em>
</p>

<p align="center">
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-_red.svg"></a>
  <a href="https://pypi.org/project/fastlog-io"><img src="https://img.shields.io/pypi/v/fastlog-io" alt="Package version"></a>
  <a href="https://pypi.org/project/fastlog-io"><img src="https://img.shields.io/pypi/status/fastlog-io" alt="Development Status"></a>
  <a href="https://pypi.org/project/fastlog-io"><img src="https://img.shields.io/pypi/pyversions/fastlog-io" alt="Supported Python versions"></a>
  <a href="https://github.com/astral-sh/ruff"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json" alt="Ruff"></a>
</p>


---

## ✨ Features

* **Zero‑config logger** – `from fastlog import log; log.info("hello")` works instantly.
* **Automatic `trace_id`** – NanoID is injected when none is bound, ideal for tracing.
* **Configurable rotation & retention** – ship with sane defaults (`100 MB` rotation) and tweak via `configure()`.
* **Stdlib interception** – `reset_std_logging()` routes the standard `logging` module through fastlog.
* **Directory watcher & notifier** – `MultiLogWatcher` tails rotating `*.log` families, persists offsets, and forwards new lines over HTTP and/or Telegram.

---

## 🚀 Installation

```bash
pip install fastlog-io
```

**Requirements**

* Python ≥ 3.12
* [loguru](https://pypi.org/project/loguru/)

---

## ⚡ Quickstart

```python
import logging

from fastlog import configure, log

# override defaults if needed
configure(
    level='DEBUG',
    log_path='./logs/app.log',
    rotation='10 MB',
    retention='7 days',
)

log.info('service started')

# standard logging is intercepted automatically
logging.getLogger('my-app').warning('cache miss')

# add trace context
with log.trace_ctx():
    log.error('request failed')
    # use custom actions
    log.bind(action='api.call').info('request done')
```

Console output (colours stripped):

```
2025-10-13 12:23:00.193 | INFO     | 3hAhb3OFpU7zXKWBwp | ts.<module>:13 | service started
2025-10-13 12:23:00.194 | WARNING  | 1F1FwLwm4Orok0gs0a | [my-app]ts.<module>:16 | cache miss
2025-10-13 12:23:00.194 | ERROR    | WG8yiDxzO3MCmWy8CB | ts.<module>:20 | request failed
2025-10-13 12:23:00.194 | INFO     | WG8yiDxzO3MCmWy8CB | api.call     | request done
```

---

## 🖥️ Directory watcher CLI

FastLOG ships with a CLI that tails a directory of `*.log` files, resumes from persisted byte offsets, and forwards high‑severity bursts.

Run `fastlog` with **no arguments** (or `fastlog --help`) to print grouped usage: **Watcher**, **Destinations**, **Batching**, plus examples.

```bash
uv tool install fastlog-io

# Watch ./logs, post notifications to HTTP and Telegram (--min-level defaults to WARNING)
uv run fastlog ./logs \
  --endpoint "$FASTLOG_NOTIFY_ENDPOINT"

# Or send to Telegram
uv run fastlog ./logs \
  --tg-token "$FASTLOG_NOTIFY_TG_TOKEN" \
  --tg-chat-id "$FASTLOG_NOTIFY_TG_CHAT_ID"

# Only ERROR and above
# uv run fastlog ./logs ... --min-level ERROR

# Or export parsed lines as OTLP (HTTP or gRPC)

pip install "fastlog-io[otlp]"

uv run fastlog ./logs \
  --otlp-endpoint "http://127.0.0.1:4318/v1/logs"

# OTLP/gRPC (URL without :4318 or /v1/logs → gRPC by default)
uv run fastlog ./logs \
  --otlp-endpoint "https://otel-grpc.example.com"

# show all options
uv run fastlog --help
```

**OTLP check:** The repository does **not** ship a smoke script. To verify a collector, run the watcher on a directory that contains a `*.log` file with Loguru’s five-field lines (see Quickstart output), or use a short local `python -c` that imports `fastlog.otlp_export.create_otlp_log_exporter` and `build_read_write_records`. If you keep a personal script under `scripts/`, name it e.g. `scripts/smoke_otlp_remote.py`—that pattern is gitignored so it stays out of version control.

Key behaviour:

* HTTP, Telegram, and OTLP batches are delivered in background worker threads; `flush()` waits until queued work is done; `close()` (used on watcher shutdown) drains queues and stops workers.
* Rotations/renames are detected; offsets are stored in `.multilogwatch.state.json`.
* **`--min-level`** defaults to **WARNING** (and `$FASTLOG_NOTIFY_LEVEL` when unset). Use **`ERROR`** to forward only errors and above.
* Notifications are batched, deduplicated, and retried per transport (default 3 attempts for HTTP/Telegram; `--http-attempts` or `$FASTLOG_HTTP_ATTEMPTS`).
* Telegram delivery is optional—provide both `--tg-token` and `--tg-chat-id` (or matching env vars).
* OTLP export is optional—install `fastlog-io[otlp]`, set `--otlp-endpoint` (or `$FASTLOG_OTLP_ENDPOINT` / `$OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`). Use `--otlp-protocol` or `$FASTLOG_OTLP_PROTOCOL` to force `http` or `grpc`; auto picks HTTP when the URL contains `/v1/logs` or `:4318`, otherwise gRPC.
* **OpenTelemetry versions:** the `[otlp]` extra pins **OpenTelemetry Python SDK, OTLP HTTP, and OTLP gRPC exporters to ≥ 1.27.0** (1.x). Use a compatible OTLP collector; upgrade the extra if you need a newer OTel minor.

## 🔧 Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `LOG_PATH` | *(empty)* | Log file path; leave blank to log to stderr only |
| `LOG_LEVEL` | `INFO` | Minimum log level for `configure()` |
| `LOG_ROTATION` | `100 MB` | File rotation policy (Loguru syntax) |
| `FASTLOG_NOTIFY_ENDPOINT` | *(empty)* | HTTP endpoint used by the CLI watcher |
| `FASTLOG_NOTIFY_LEVEL` | `WARNING` | Watcher `--min-level` default when unset; set to override (e.g. `ERROR`) |
| `FASTLOG_NOTIFY_TIMEOUT` | *(empty)* | HTTP timeout override (seconds) |
| `FASTLOG_HTTP_ATTEMPTS` | *(empty)* | HTTP/Telegram POST retries (default 3; unset keeps 3) |
| `FASTLOG_WINDOW_SECONDS` | `30` | Batch aggregation window in seconds for OTLP, HTTP, and Telegram |
| `FASTLOG_NOTIFY_MAX_BYTES` | `4096` | Maximum payload size per request |
| `FASTLOG_NOTIFY_TG_TOKEN` | *(empty)* | Telegram bot token for watcher notifications |
| `FASTLOG_NOTIFY_TG_CHAT_ID` | *(empty)* | Telegram chat ID for watcher notifications |
| `FASTLOG_OTLP_ENDPOINT` | *(empty)* | OTLP collector URL for the watcher (HTTP or gRPC) |
| `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` | *(empty)* | Standard OTel env; used if `FASTLOG_OTLP_ENDPOINT` is unset |
| `FASTLOG_OTLP_PROTOCOL` | *(empty)* | `grpc` or `http`; when unset, CLI `--otlp-protocol auto` uses heuristics |
| `OTEL_SERVICE_NAME` | *(empty)* | Overrides `--otlp-service-name`; default resource `service.name` is `fastlog` |
| `FASTLOG_OTLP_MAX_BATCH` | `256` | Max records per OTLP export |

Variables can also be supplied as CLI flags; command‑line arguments take precedence over environment values.

---

## 🛠 Development & Tests (powered by [uv](https://github.com/astral-sh/uv))

```bash
# run tests in a temp venv with test extras installed
uv run --with '.[test]' pytest -q

# build wheel + sdist
uv build

# verify the built wheel in a clean env
uv run --with dist/*.whl --with pytest pytest -q
```

---

## 📄 License

[MIT](LICENSE) © 2025 OWQ
