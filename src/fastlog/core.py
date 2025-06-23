import sys
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from types import MethodType
from typing import ContextManager, Iterator, cast

from loguru import logger as _logger
from loguru._logger import Logger as _Logger
from loguru._logger import context as _context

from .config import Config
from .helpers import generate_id
from .intercept import reset_std_logging
from .metrics import metrics_patch

__all__ = ['configure', 'get_log', 'trace_ctx']

logger = _logger


# --------------------------------------------------------------------------- #
# Record patches
# --------------------------------------------------------------------------- #


def _patch_trace_and_name(record: dict) -> None:
    trace_id = record['extra'].get('trace_id')
    sub_trace_id = record['extra'].get('sub_trace_id')
    if trace_id and sub_trace_id:
        trace_id = f'{trace_id}:{sub_trace_id}'
    elif sub_trace_id:
        trace_id = sub_trace_id
    elif not trace_id:
        trace_id = f'-{generate_id(7)}'
    record['extra']['trace_id'] = trace_id
    record['extra'].setdefault('name', '-')


# --------------------------------------------------------------------------- #
# Trace Func
# --------------------------------------------------------------------------- #


@contextmanager
def _trace_ctx(self: _Logger, trace_id: str | None = None) -> Iterator[None]:
    trace_id = trace_id or generate_id(8)
    if _context.get().get('trace_id'):
        with self.contextualize(sub_trace_id=trace_id):
            yield
    else:
        with self.contextualize(trace_id=trace_id):
            yield


_Logger.trace_ctx = MethodType(_trace_ctx, logger)


# Add static type checking support
class Logger(_Logger):
    def trace_ctx(self, trace_id: str | None = None) -> ContextManager[None]: ...


logger = cast(Logger, logger)


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

_configure: bool = False

def configure(
    log_dir: str | Path | None = None,
    level: str | None = None,
    rotation: str | None = None,
    retention: str | None = None,
) -> None:
    cfg_kwargs = dict()
    if log_dir is not None:
        cfg_kwargs['log_dir'] = Path(log_dir)
    if level is not None:
        cfg_kwargs['level'] = level.upper()
    if rotation is not None:
        cfg_kwargs['rotation'] = rotation
    if retention is not None:
        cfg_kwargs['retention'] = retention

    cfg = Config(**cfg_kwargs)

    global logger

    logger.remove()

    logger = logger.patch(_patch_trace_and_name).patch(metrics_patch)

    logger.add(
        sys.stderr,
        format=cfg.format,
        level=cfg.level,
        colorize=True,
        enqueue=False,
    )

    if cfg.log_dir:
        enqueue = sys.platform.startswith('linux')
        logger.add(
            cfg.log_dir / 'app.log',
            format=cfg.format,
            level=cfg.level,
            rotation=cfg.rotation,
            retention=cfg.retention,
            enqueue=enqueue,
        )

    reset_std_logging()

    global _configure
    _configure = True

@lru_cache
def get_log(name: str | None = None):
    if not _configure:
        configure()
    return logger.bind(name=name) if name else logger
