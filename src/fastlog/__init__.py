import atexit as _atexit
from .core import configure, logger

__all__ = ['configure', 'log', 'cli']

log = logger

configure()

_atexit.register(log.remove)
