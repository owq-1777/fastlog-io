def test_basic_logging_to_stderr(tmp_path):
    from fastlog import configure, log
    from fastlog.core import logger

    log_file = tmp_path / 't.log'
    configure(log_path=str(log_file))
    log.info('hello stderr')
    logger.complete()
    text = log_file.read_text(encoding='utf-8')
    assert 'hello stderr' in text


def test_bound_extra_is_written_as_structured_field(tmp_path):
    from fastlog import configure, log
    from fastlog.core import logger

    log_file = tmp_path / 't.log'
    configure(log_path=str(log_file))
    log.bind(send_msg=1, chat_id='ops-room').info('hello extra')
    logger.complete()
    text = log_file.read_text(encoding='utf-8')
    assert '{"chat_id":"ops-room","send_msg":1}' in text
