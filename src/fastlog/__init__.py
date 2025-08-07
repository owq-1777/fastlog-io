import atexit as _atexit
from .core import configure, logger

__all__ = ['configure', 'log']

log = logger

configure()

_atexit.register(log.remove)
