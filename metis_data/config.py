from typing import Any
import logging
import re
from dataclasses import dataclass, field
from enum import Enum

from metis_data import namespace, repo, const
from metis_data.namespace import logging_ns
from metis_data.util import logger

normalise_pattern = pattern = re.compile(r'(?<!^)(?=[A-Z])')
logging_ns = f"{const.NS}.config"

def normalise(token):
    if not token:
        return token
    return normalise_pattern.sub('_', token).lower()


class CatalogueMode(Enum):
    SPARK = "spark"
    UNITY = "unity"


@dataclass
class Config:
    catalogue: str
    data_product: str
    service_name: str
    owner: str | None = None
    catalogue_mode: CatalogueMode = field(default_factory=lambda: CatalogueMode.UNITY)
    checkpoint_volume: repo.CheckpointVolumeRoot | None = None
    namespace_strategy_cls: namespace.CatalogueStrategyProtocol | None = None
    log_level: int = logging.INFO
    with_logger: Any = None
    log_cfg: logger.LogConfig = None

    def __post_init__(self):
        self.catalogue = normalise(self.catalogue)
        self.service_name = normalise(self.service_name)
        self.data_product = normalise(self.data_product)
        self.log_cfg = logger.LogConfig()
        if not self.with_logger:
            self.log_cfg.level = self.log_level
        else:
            self.log_cfg.logger = self.with_logger
            self.log_cfg.level = self.log_level
        logger.info(f"{logging_ns}.loggingConfig",
                    logger_cls=self.log_cfg.logger.__class__.__name__,
                    logger_level=self.log_cfg.level)

    @property
    def configured_logger(self):
        return self.log_cfg.logger