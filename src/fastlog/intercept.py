import logging

from .core import logger
from .util import generate_id


class InterceptHandler(logging.Handler):
    """A handler that forwards standard logging records to Loguru."""

    _cache: dict[str, str] = {}

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).bind(
            action=f'[{record.name}]{record.module}.{record.funcName}:{record.lineno}',
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
