import logging

from metis_data.util import logger


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