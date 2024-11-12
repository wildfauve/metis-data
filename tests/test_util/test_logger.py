import logging
from functools import partial

from metis_data import config
from metis_data.util import logger
from pymonad.maybe import Just, Nothing, Maybe


class NoopLogger():
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

    lgr = cfg.logger.logger

    assert isinstance(cfg.logger.logger, logger.NoopLogger)
    assert cfg.logger.logger.level == logging.INFO

    logger.info("Test1", with_ctx1="CTX1", with_ctx2="CTX2")
    logger.info("Test2", with_ctx1="CTX1", with_ctx2="CTX2")

    assert {log.get('msg') for log in lgr.logs} == {"Test1", "Test2"}

def test_noop_logger_supports_maybe():
    logger.LogConfig().reset()
    cfg = config.Config(catalogue="Cat1", data_product="DP1", service_name="Srv1")

    lgr = cfg.logger.logger
    result = Just("Val1").maybe(None, partial(logger.maybe_info, "Logging in Maybe"))

    assert result == Just("Val1")
    assert lgr.logs[0].get('msg') == "Logging in Maybe : Val1"


def test_inject_alternate_logger():
    logger.LogConfig().reset()
    lgr = NoopLogger(level=logging.INFO)
    cfg = config.Config(catalogue="Cat1", data_product="DP1", service_name="Srv1", logger=lgr)

    logger.info("Test1", with_ctx1="CTX1", with_ctx2="CTX2")
    logger.info("Test2", with_ctx1="CTX1", with_ctx2="CTX2")

    assert {log.get('msg') for log in lgr.logs} == {"Test1", "Test2"}


