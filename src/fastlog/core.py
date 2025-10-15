import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping

import logging

from loguru._logger import Logger as _Logger
from loguru._logger import Core as _Core
from loguru._logger import context as _context

from .config import Config
from .util import generate_id

__all__ = ['configure']


class Logger(_Logger):
    def __init__(self, core, exception, depth, record, lazy, colors, raw, capture, patchers, extra):
        super().__init__(core, exception, depth, record, lazy, colors, raw, capture, patchers, extra)

    def bind(__self, **kwargs):  # noqa: N805
        *options, extra = __self._options
        return Logger(__self._core, *options, extra={**extra, **kwargs})

    @contextmanager
    def trace_ctx(self, trace_id: str | None = None, **keyword):
        trace_id = trace_id or generate_id()
        if _context.get().get('trace_id'):
            with self.contextualize(sub_trace_id=trace_id, **keyword):
                yield
        else:
            with self.contextualize(trace_id=trace_id, **keyword):
                yield


# --------------------------------------------------------------------------- #
# Logging Intercept
# --------------------------------------------------------------------------- #


class InterceptHandler(logging.Handler):
    """A handler that forwards standard logging records to Loguru."""

    _cache: dict[str, str] = {}

    def emit(self, record: logging.LogRecord) -> None:  # noqa: D401
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back  # type: ignore[assignment]
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).bind(
            action=f'[{record.name}]{record.module}.{record.funcName}:{record.lineno}'.replace('<', '\\<'),
            trace_id=self._track_id(record.name),
        ).log(level, record.getMessage())

    @classmethod
    def _track_id(cls, name: str) -> str:
        if name not in cls._cache:
            cls._cache[name] = generate_id()
        return cls._cache[name]


def reset_std_logging() -> None:
    """Replace the root logger's handlers with a single `InterceptHandler`."""
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(InterceptHandler())
    root.setLevel(logging.WARNING)


def reset_fastapi_logging() -> None:
    loggers = (
        'uvicorn',
        'uvicorn.access',
        'uvicorn.error',
        'fastapi',
        'asyncio',
        'starlette',
    )

    for logger_name in loggers:
        logging_logger = logging.getLogger(logger_name)
        logging_logger.handlers.clear()
        logging_logger.propagate = True


# --------------------------------------------------------------------------- #
# Patch
# --------------------------------------------------------------------------- #


def patch_trace(record: Mapping[str, Any]) -> None:
    trace_id = record['extra'].get('trace_id')
    sub_trace_id = record['extra'].get('sub_trace_id')
    if trace_id and sub_trace_id:
        trace_id = f'{trace_id}:{sub_trace_id}'
    elif sub_trace_id:
        trace_id = sub_trace_id
    elif not trace_id:
        trace_id = generate_id()
    record['extra']['trace_id'] = trace_id


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #


logger = Logger(
    core=_Core(),
    exception=None,
    depth=0,
    record=False,
    lazy=False,
    colors=False,
    raw=False,
    capture=True,
    patchers=[patch_trace],
    extra={},
)


def configure(
    level: str | None = None,
    log_path: str | Path | None = None,
    rotation: str | None = None,
    retention: str | None = None,
) -> None:
    cfg_kwargs = dict()
    if level is not None:
        cfg_kwargs['level'] = level.upper()
    if log_path is not None:
        cfg_kwargs['log_path'] = Path(log_path)
    if rotation is not None:
        cfg_kwargs['rotation'] = rotation
    if retention is not None:
        cfg_kwargs['retention'] = retention

    cfg = Config(**cfg_kwargs)

    def format_record(record: Mapping[str, Any]) -> str:
        loguru_format = cfg.format
        if action := record['extra'].get('action'):
            loguru_format = loguru_format.replace(cfg.action_format, f'{action: <12}')

        return loguru_format

    logger.remove()

    logger.add(
        sys.stderr,
        format=format_record,
        level=cfg.level,
        colorize=True,
        enqueue=False,
    )

    if cfg.log_path:
        enqueue = sys.platform.startswith('linux')
        logger.add(
            cfg.log_path,
            format=format_record,
            level=cfg.level,
            rotation=cfg.rotation,
            retention=cfg.retention,
            enqueue=enqueue,
        )

    reset_std_logging()
    reset_fastapi_logging()
