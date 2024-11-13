import logging

from metis_data.util import logger

from .shared import *


class MockLogger:

    def __init__(self):
        self.msgs = []

    def info(self, meta, msg):
        self.msgs.append((meta, msg))

    def debug(self, meta, msg):
        self.msgs.append((meta, msg))



def it_create_a_job_config():
    cfg = metis_data.Config(catalogue="my_domain",
                            data_product="my_data_product",
                            service_name="my_service")

    assert cfg.catalogue == "my_domain"


def it_normalises_names_to_snake_case():
    cfg = metis_data.Config(catalogue="myDomain",
                            data_product="MyDataProduct",
                            service_name="myService")

    assert cfg.catalogue == "my_domain"


def it_supports_a_custom_logger():
    mock_logger = MockLogger()
    cfg = metis_data.Config(catalogue="myDomain",
                            data_product="MyDataProduct",
                            service_name="myService",
                            log_level=logging.DEBUG,
                            with_logger=mock_logger)
    assert logger.LogConfig().logger == mock_logger

    logger.info("a test")

    assert len(mock_logger.msgs) == 2

# Helpers
