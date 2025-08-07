<p align="center">
  <br>
  <a href="https://github.com/owq-1777/fastlog-io">
  	<img src="https://img.owq.world/2025/fastlog-logo-857f7d.png" alt="FastLOG">
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

* **Zero‑config out of the box** – `from fastlog-io import log; log.info("hello")` works instantly.
* **Unified interface** – `configure()` + `get_log(name)` keep formatting consistent across modules.
* **Automatic `trace_id`** – generates a 7‑char NanoID when none is bound, perfect for request tracing.
* **File rotation & retention** – default `rotation="100 MB"`; tune it via `configure()`.
* **Stdlib compatibility** – built‑in `reset_std_logging()` redirects the `logging` module to fastlog-io.

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
from fastlog import log, configure

# override defaults if needed
configure(level="DEBUG", log_dir="./logs")

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