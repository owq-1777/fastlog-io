def test_basic_logging_to_stderr(capfd):
    from fastlog import log

    log.info('hello stderr')

    out, err = capfd.readouterr()
    assert 'hello stderr' in err
    assert out == ''
