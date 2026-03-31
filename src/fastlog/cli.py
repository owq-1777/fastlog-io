from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Sequence

from .log_handle import LogNotificationHandler
from .monitor import MultiLogWatcher


class _HelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Preserve epilog layout; defaults are described in help text where useful."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='fastlog',
        description='Tail *.log files under LOG_DIR and forward lines in batches (HTTP, Telegram, and/or OTLP).',
        formatter_class=_HelpFormatter,
        epilog='''
Examples:
  fastlog ./logs --endpoint http://localhost:8080/notify
  fastlog ./logs --otlp-endpoint https://collector.example.com
  fastlog ./logs --tg-token "$TG_TOKEN" --tg-chat-id "$CHAT_ID"

At least one of --endpoint, (--tg-token and --tg-chat-id), or --otlp-endpoint is required.
OTLP needs: pip install "fastlog-io[otlp]". Batches are delivered in background threads; shutdown drains queues.
HTTP/Telegram retries: --http-attempts (default 3) or $FASTLOG_HTTP_ATTEMPTS. See README for all env vars.
''',
    )

    watcher = parser.add_argument_group('Watcher', 'What to tail and where to store offsets.')
    watcher.add_argument(
        'log_dir',
        nargs='?',
        default=None,
        metavar='LOG_DIR',
        help='Directory containing *.log files to monitor.',
    )
    watcher.add_argument(
        '--state',
        default=None,
        help='State file path (default: <LOG_DIR>/.multilogwatch.state.json).',
    )

    dest = parser.add_argument_group('Destinations', 'Provide at least one transport.')
    dest.add_argument(
        '--endpoint',
        default=os.getenv('FASTLOG_NOTIFY_ENDPOINT'),
        help='Plain HTTP POST URL for notifications ($FASTLOG_NOTIFY_ENDPOINT).',
    )
    dest.add_argument(
        '--tg-token',
        default=os.getenv('FASTLOG_NOTIFY_TG_TOKEN'),
        help='Telegram bot token ($FASTLOG_NOTIFY_TG_TOKEN).',
    )
    dest.add_argument(
        '--tg-chat-id',
        default=os.getenv('FASTLOG_NOTIFY_TG_CHAT_ID'),
        help='Telegram chat id ($FASTLOG_NOTIFY_TG_CHAT_ID).',
    )
    dest.add_argument(
        '--otlp-endpoint',
        default=os.getenv('FASTLOG_OTLP_ENDPOINT') or os.getenv('OTEL_EXPORTER_OTLP_LOGS_ENDPOINT'),
        help='OTLP collector URL (HTTP …/v1/logs or gRPC https://host); requires fastlog-io[otlp].',
    )
    dest.add_argument(
        '--otlp-protocol',
        default='auto',
        choices=['auto', 'http', 'grpc'],
        help='OTLP transport; auto uses HTTP if URL has /v1/logs or :4318, else gRPC.',
    )
    dest.add_argument(
        '--otlp-service-name',
        default=os.getenv('OTEL_SERVICE_NAME') or 'fastlog',
        help='OTLP resource service.name (default: fastlog; override with $OTEL_SERVICE_NAME).',
    )
    dest.add_argument(
        '--otlp-max-batch',
        default=os.getenv('FASTLOG_OTLP_MAX_BATCH', '256'),
        help='Max log records per OTLP export.',
    )

    batch = parser.add_argument_group('Batching', 'When and how much to send per batch.')
    batch.add_argument(
        '--min-level',
        default=os.getenv('FASTLOG_NOTIFY_LEVEL', 'WARNING'),
        help='Minimum level for primary lines (default: WARNING, or $FASTLOG_NOTIFY_LEVEL).',
    )
    batch.add_argument(
        '--timeout',
        default=os.getenv('FASTLOG_NOTIFY_TIMEOUT'),
        help='HTTP/OTLP request timeout in seconds (default: 5).',
    )
    batch.add_argument(
        '--window-seconds',
        default=os.getenv('FASTLOG_WINDOW_SECONDS', '30'),
        help='Batch aggregation window in seconds for all transports (default: 30, or $FASTLOG_WINDOW_SECONDS).',
    )
    batch.add_argument(
        '--max-bytes',
        default=os.getenv('FASTLOG_NOTIFY_MAX_BYTES', '4096'),
        help='Max payload size per plain HTTP POST in bytes (default: 4096).',
    )
    batch.add_argument(
        '--http-attempts',
        default=None,
        type=int,
        metavar='N',
        help='HTTP/Telegram POST retry attempts (default: 3, or $FASTLOG_HTTP_ATTEMPTS).',
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    argv_list = list(sys.argv[1:]) if argv is None else list(argv)
    args = parser.parse_args(argv_list)

    if args.log_dir is None:
        if not argv_list:
            parser.print_help()
            return 0
        parser.error('LOG_DIR is required (directory with *.log files). Run fastlog with no arguments for help.')

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

    endpoint = (args.endpoint or '').strip() or None
    otlp_endpoint = (args.otlp_endpoint or '').strip() or None

    if not endpoint and not (args.tg_token and args.tg_chat_id) and not otlp_endpoint:
        parser.error('Provide --endpoint, or both --tg-token and --tg-chat-id, or --otlp-endpoint.')

    try:
        timeout = float(args.timeout) if args.timeout is not None else 5.0
    except ValueError as exc:
        parser.error(f'Invalid --timeout value: {exc}')

    try:
        window_seconds = float(args.window_seconds)
    except ValueError as exc:
        parser.error(f'Invalid --window-seconds value: {exc}')
    if window_seconds <= 0:
        parser.error('--window-seconds must be positive')

    try:
        max_bytes = int(args.max_bytes)
    except ValueError as exc:
        parser.error(f'Invalid --max-bytes value: {exc}')
    if max_bytes <= 0:
        parser.error('--max-bytes must be a positive integer')

    try:
        otlp_max_batch = int(args.otlp_max_batch)
    except ValueError as exc:
        parser.error(f'Invalid --otlp-max-batch value: {exc}')
    if otlp_max_batch <= 0:
        parser.error('--otlp-max-batch must be a positive integer')

    http_attempts = args.http_attempts
    if http_attempts is None:
        env_ha = os.getenv('FASTLOG_HTTP_ATTEMPTS')
        if env_ha is not None and env_ha.strip() != '':
            try:
                http_attempts = int(env_ha)
            except ValueError as exc:
                parser.error(f'Invalid FASTLOG_HTTP_ATTEMPTS: {exc}')
        else:
            http_attempts = 3
    if http_attempts < 1:
        parser.error('--http-attempts must be >= 1')

    otlp_protocol = None if args.otlp_protocol == 'auto' else args.otlp_protocol

    try:
        handler = LogNotificationHandler(
            endpoint=endpoint,
            min_level=args.min_level,
            timeout=timeout,
            window_seconds=window_seconds,
            max_bytes=max_bytes,
            telegram_token=args.tg_token,
            telegram_chat_id=args.tg_chat_id,
            otlp_endpoint=otlp_endpoint,
            otlp_service_name=args.otlp_service_name,
            otlp_max_batch=otlp_max_batch,
            otlp_protocol=otlp_protocol,
            http_attempts=http_attempts,
        )
    except ImportError as exc:
        print(f'fastlog: {exc}', file=sys.stderr)
        print('Install OTLP support: pip install "fastlog-io[otlp]"', file=sys.stderr)
        print('Or: uv sync --extra otlp  (when developing this package)', file=sys.stderr)
        return 2

    watcher = MultiLogWatcher(
        dirpath=args.log_dir,
        state_path=state_path,
        handler=handler,
    )

    try:
        watcher.start()
    except KeyboardInterrupt:
        print('Stopped by user (Ctrl+C).', file=sys.stderr)
    return 0
