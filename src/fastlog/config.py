import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    """Holds runtime configuration for logging."""

    # Minimum log level. Can be overwritten with $LOG_LEVEL
    level: str = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_path: Path | None = Path(str(os.getenv('LOG_PATH'))) if os.getenv('LOG_PATH') else None
    # Loguru rotation policy (e.g. '100 MB' or '1 day').
    rotation: str = os.getenv('LOG_ROTATION', '100 MB')
    # Loguru retention policy (e.g. '30 days' or '10' files).
    retention: str | int = os.getenv('LOG_RETENTION', 3)

    format: str = (
        '<green>{time:YYYY-MM-DD HH:mm:ss.SSS!UTC}</green> | '
        '<level>{level:<8}</level> | '
        '<light-black>{extra[trace_id]}</light-black> | '
        '<cyan>{module}.{function}:{line}</cyan> | '
        '<level>{message}</level>'
        '\n{exception}'
    )

    action_format: str = '{module}.{function}:{line}'
