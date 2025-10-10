from __future__ import annotations

import argparse
import os
import sys
import logging
from typing import Sequence

from .log_handle import LogNotificationHandler
from .monitor import MultiLogWatcher


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run the fastlog multi-log watcher from the command line.')
    parser.add_argument('log_dir', help='Directory containing *.log files to monitor.')
    parser.add_argument(
        '--state',
        default=None,
        help='Optional path for the watcher state file. Defaults to <log_dir>/.multilogwatch.state.json.',
    )
    parser.add_argument(
        '--endpoint',
        default=os.getenv('FASTLOG_NOTIFY_ENDPOINT'),
        help='HTTP endpoint for delivering log notifications. Defaults to $FASTLOG_NOTIFY_ENDPOINT if set.',
    )
    parser.add_argument(
        '--tg-token',
        default=os.getenv('FASTLOG_NOTIFY_TG_TOKEN'),
        help='Optional Telegram bot token (or $FASTLOG_NOTIFY_TG_TOKEN).',
    )
    parser.add_argument(
        '--tg-chat-id',
        default=os.getenv('FASTLOG_NOTIFY_TG_CHAT_ID'),
        help='Optional Telegram chat ID (or $FASTLOG_NOTIFY_TG_CHAT_ID).',
    )
    parser.add_argument(
        '--min-level',
        default=os.getenv('FASTLOG_NOTIFY_LEVEL'),
        help='Minimum log level to forward (e.g. INFO, WARNING). '
        'Defaults to Config.level or $FASTLOG_NOTIFY_LEVEL if provided.',
    )
    parser.add_argument(
        '--timeout',
        default=os.getenv('FASTLOG_NOTIFY_TIMEOUT'),
        help='HTTP POST timeout in seconds (default: 5.0 or $FASTLOG_NOTIFY_TIMEOUT).',
    )
    parser.add_argument(
        '--window-minutes',
        default=os.getenv('FASTLOG_NOTIFY_WINDOW_MINUTES', '1.0'),
        help='Aggregation window length in minutes before sending notifications (default: 1.0).',
    )
    parser.add_argument(
        '--max-bytes',
        default=os.getenv('FASTLOG_NOTIFY_MAX_BYTES', '4096'),
        help='Maximum payload size per POST in bytes (default: 4096).',
    )
    parser.add_argument(
        '--fast-interval',
        type=float,
        default=0.05,
        help='Fast polling interval in seconds when new data is detected.',
    )
    parser.add_argument(
        '--slow-interval',
        type=float,
        default=1.0,
        help='Maximum backoff polling interval in seconds.',
    )
    parser.add_argument(
        '--backoff',
        type=float,
        default=2.0,
        help='Exponential backoff factor applied when no data is available.',
    )
    parser.add_argument(
        '--flush-interval',
        type=float,
        default=1.0,
        help='Minimum seconds between stdout flush attempts.',
    )
    parser.add_argument(
        '--rr-batch',
        type=int,
        default=64,
        help='Maximum number of lines read per family in a single round.',
    )
    parser.add_argument(
        '--state-flush-interval',
        type=float,
        default=0.8,
        help='Minimum seconds between state persistence flushes.',
    )
    parser.add_argument(
        '--read-chunk',
        type=int,
        default=8192,
        help='Binary chunk size used when reading the log files.',
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(None if argv is None else list(argv))

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    state_path = args.state or os.path.join(
        os.path.abspath(args.log_dir),
        '.multilogwatch.state.json',
    )
    os.makedirs(os.path.dirname(state_path) or '.', exist_ok=True)

    if not args.endpoint and not (args.tg_token and args.tg_chat_id):
        parser.error('Provide either --endpoint or both --tg-token and --tg-chat-id.')

    try:
        timeout = float(args.timeout) if args.timeout is not None else 5.0
    except ValueError as exc:
        parser.error(f'Invalid --timeout value: {exc}')

    try:
        window_minutes = float(args.window_minutes)
    except ValueError as exc:
        parser.error(f'Invalid --window-minutes value: {exc}')
    if window_minutes <= 0:
        parser.error('--window-minutes must be positive')

    try:
        max_bytes = int(args.max_bytes)
    except ValueError as exc:
        parser.error(f'Invalid --max-bytes value: {exc}')
    if max_bytes <= 0:
        parser.error('--max-bytes must be a positive integer')

    handler = LogNotificationHandler(
        endpoint=args.endpoint,
        min_level=args.min_level,
        timeout=timeout,
        window_minutes=window_minutes,
        max_bytes=max_bytes,
        telegram_token=args.tg_token,
        telegram_chat_id=args.tg_chat_id,
    )

    watcher = MultiLogWatcher(
        dirpath=args.log_dir,
        state_path=state_path,
        fast_interval=args.fast_interval,
        slow_interval=args.slow_interval,
        backoff=args.backoff,
        flush_interval=args.flush_interval,
        rr_batch=args.rr_batch,
        state_flush_interval=args.state_flush_interval,
        read_chunk=args.read_chunk,
        handler=handler,
    )

    try:
        watcher.start()
    except KeyboardInterrupt:
        print('Stopped by user (Ctrl+C).', file=sys.stderr)
    return 0
