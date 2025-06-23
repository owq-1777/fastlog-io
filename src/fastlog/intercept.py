import logging

from loguru import logger

from .helpers import generate_id
class InterceptHandler(logging.Handler):
    """ A handler that forwards standard logging records to Loguru. """

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
            action=f'[{record.name}]{record.module}.{record.funcName}:{record.lineno}',
            trace_id=self._track_id(record.name),
        ).log(level, record.getMessage())

    @classmethod
    def _track_id(cls, name: str) -> str:
        if name not in cls._cache:
            cls._cache[name] = f'-{generate_id(6, digits=True)}-'
        return cls._cache[name]


def reset_std_logging() -> None:
    """ Replace the root logger's handlers with a single `InterceptHandler`. """
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(InterceptHandler())
    root.setLevel(logging.WARNING)


def reset_uvicorn_logging() -> None:
    # Capture the uvicorn log correctly to prevent duplicate output
    for name in ('uvicorn', 'uvicorn.error', 'uvicorn.access'):
        logging.getLogger(name).handlers.clear()
        logging.getLogger(name).propagate = False
    logging.getLogger('uvicorn').handlers = [InterceptHandler()]
