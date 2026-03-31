def test_basic_logging_to_stderr(tmp_path):
    from fastlog import configure, log
    from fastlog.core import logger

    log_file = tmp_path / 't.log'
    configure(log_path=str(log_file))
    log.info('hello stderr')
    logger.complete()
    text = log_file.read_text(encoding='utf-8')
    assert 'hello stderr' in text
