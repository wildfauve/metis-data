import logging
from functools import partial

from metis_data import config
from metis_data.util import logger
from pymonad.maybe import Just, Nothing, Maybe


class MockLogger:
    def __init__(self, level):
        self.logs = []
        self.level = level

    def info(self,
             meta,
             msg: str) -> None:
        self.logs.append({**{"msg": msg}, **meta})
        pass


def it_is_a_singleton():
    cfg1 = logger.LogConfig()
    cfg2 = logger.LogConfig()

    assert id(cfg1) == id(cfg2)


def it_sets_the_level():
    cfg = logger.LogConfig()
    cfg.level = logging.INFO
    assert cfg.level == logging.INFO

    cfg.level = logging.DEBUG
    assert cfg.level == logging.DEBUG


def it_uses_a_noop_logger_when_no_logger_configured():
    logger.LogConfig().reset()
    cfg = config.Config(catalogue="Cat1", data_product="DP1", service_name="Srv1")

    lgr = cfg.log_cfg.logger

    assert isinstance(lgr, logger.NoopLogger)
    assert lgr.level == logging.INFO

    logger.info("Test1", with_ctx1="CTX1", with_ctx2="CTX2")
    logger.info("Test2", with_ctx1="CTX1", with_ctx2="CTX2")

    assert {log.get('msg') for log in lgr.logs} == {"metisData.config.loggingConfig", "Test1", "Test2"}


def test_noop_logger_supports_maybe():
    logger.LogConfig().reset(level=logging.DEBUG)

    result = Just("Val1").maybe(None, partial(logger.maybe_info, "Logging in Maybe"))

    assert result == Just("Val1")
    assert logger.LogConfig().logger.logs[0].get('msg') == "Logging in Maybe : Val1"


def test_inject_alternate_logger():
    logger.LogConfig().reset(MockLogger(level=logging.INFO), logging.INFO)
    cfg = config.Config(catalogue="Cat1", data_product="DP1", service_name="Srv1", with_logger=logger.LogConfig().logger)

    logger.info("Test1", with_ctx1="CTX1", with_ctx2="CTX2")
    logger.info("Test2", with_ctx1="CTX1", with_ctx2="CTX2")

    assert {log.get('msg') for log in logger.LogConfig().logger.logs} == {"metisData.config.loggingConfig", "Test1", "Test2"}


def test_job_config_resets_logger():
    logger.LogConfig().reset(level=logging.DEBUG)
    logger.info("Logger1")

    assert {log.get('msg') for log in logger.LogConfig().logger.logs} == {"Logger1"}


    mock_lgr = MockLogger(level=logging.INFO)
    config.Config(catalogue="Cat1", data_product="DP1", service_name="Srv1", with_logger=mock_lgr)

    logger.info("Logger2")
    assert {log.get('msg') for log in logger.LogConfig().logger.logs} == {"metisData.config.loggingConfig", "Logger2"}
