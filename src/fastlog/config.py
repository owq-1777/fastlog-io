import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    '''Holds runtime configuration for logging.'''

    # Directory for log files. Defaults to None or $LOG_DIR env var
    log_dir: Path | None = Path(os.getenv('LOG_DIR')) if os.getenv('LOG_DIR') else None
    # Minimum log level. Can be overwritten with $LOG_LEVEL
    level: str = os.getenv('LOG_LEVEL', 'INFO').upper()
    # Loguru rotation policy (e.g. '100 MB' or '1 day').
    rotation: str = os.getenv('LOG_ROTATION', '100 MB')
    # Loguru retention policy (e.g. '30 days' or '10 files').
    retention: str = os.getenv('LOG_RETENTION', '90 days')

    format: str = (
        '<green>{time:YYYY-MM-DD HH:mm:ss.SSS!UTC}</green> | '
        '<level>{level:<8}</level> | '
        '<yellow>{extra[name]:<5}</yellow> | '
        '<light-black>{extra[trace_id]}</light-black> | '
        '<cyan>{module}.{function}:{line}</cyan> | '
        '<level>{message}</level>{exception}'
    )
