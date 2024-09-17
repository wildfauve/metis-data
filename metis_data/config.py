from typing import Any
import logging
import re
from dataclasses import dataclass, field
from enum import Enum

from metis_data import namespace, repo
from metis_data.util import logger

normalise_pattern = pattern = re.compile(r'(?<!^)(?=[A-Z])')


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
    logger: Any = None


    def __post_init__(self):
        self.catalogue = normalise(self.catalogue)
        self.service_name = normalise(self.service_name)
        self.data_product = normalise(self.data_product)
        if not self.logger:
            self.logger = logger.LogConfig().configure(level=self.log_level)
        else:
            logger.LogConfig().configure(logger=self.logger, level=self.log_level)
