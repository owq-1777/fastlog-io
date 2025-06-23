<p align="center">
  <a href="https://pypi.org/project/fastlog-io" target="_blank">
  <img src="https://img.owq.world/2025/fastlog-logo-857f7d.png" alt="FastLOG">
  </a>
</p>
<p align="center">
    <em>FastLOG is a lightweight wrapper around <a href="https://github.com/Delgan/loguru" target="_blank">Loguru</a> that offers Prometheus metrics, automatic `trace_id`, colourful output, and a dead‑simple configuration API.</em>
</p>
<p align="center">
  <a href="https://pypi.org/project/fastlog-io" target="_blank">
      <img src="https://img.shields.io/pypi/v/fastlog-io" alt="Package version">
  </a>
  <span> | <span>
  <a href="https://pypi.org/project/fastlog-io" target="_blank">
      <img src="https://img.shields.io/pypi/status/fastlog-io" alt="Development Status">
  </a>
  <span> | <span>
  <a href="https://pypi.org/project/fastlog-io" target="_blank">
      <img src="https://img.shields.io/pypi/pyversions/fastlog-io" alt="Supported Python versions">
  </a>
</p>


---

## ✨ Features

* **Zero‑config out of the box** – `from fastlog-io import log; log.info("hello")` works instantly.
* **Unified interface** – `configure()` + `get_log(name)` keep formatting consistent across modules.
* **Automatic `trace_id`** – generates a 7‑char NanoID when none is bound, perfect for request tracing.
* **Prometheus ready** – one call to `start_metrics_server()` exposes `/metrics` with log counters.
* **File rotation & retention** – default `rotation="100 MB"`; tune it via `configure()`.
* **Stdlib compatibility** – built‑in `reset_std_logging()` redirects the `logging` module to fastlog-io.

---

## 🚀 Installation

```bash
pip install fastlog-io
# Use the prometheus dependency version
pip install fastlog-io[metrics]
```

**Requirements**

* Python ≥ 3.12
* [loguru](https://pypi.org/project/loguru/)
* Optional: [prometheus‑client](https://pypi.org/project/prometheus-client/) for metric export

---

## ⚡ Quickstart

```python
from fastlog import log, configure, start_metrics_server

# override defaults if needed
configure(level="DEBUG", log_dir="./logs")
start_metrics_server(port=9100)

log.info("service started")

# child logger
api_log = log.bind(name="api")
api_log.debug("new request")
```

Console output (colours stripped):

```
2025-06-21 09:57:56.510 | INFO     | app   | -cFZrY2V | main.<module>:7 | service started
2025-06-21 09:57:56.511 | DEBUG    | api   | -25bJVku | main.<module>:11 | new request
```

---

## 🔧 Environment variables

| Variable       | Default   | Description                                           |
| -------------- | --------- | ----------------------------------------------------- |
| `LOG_DIR`      | *(empty)* | Log file directory; leave blank to log to stderr only |
| `LOG_LEVEL`    | `INFO`    | Minimum log level                                     |
| `LOG_ROTATION` | `100 MB`  | File rotation policy (Loguru syntax)                  |

> These can also be passed directly to `configure()` and override environment values.

---

## 📊 Metrics

* **`log_messages_total{level, name}`** – total log messages, labelled by level and logger name.

Prometheus scrape example:

```yaml
targets:
  - job_name: fastlog-io
    static_configs:
      - targets: ["127.0.0.1:9100"]
```

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
