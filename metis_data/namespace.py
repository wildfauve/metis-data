from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import Protocol

import metis_data
from metis_data.util import logger, error
from metis_data.repo import sql_builder, properties
from . import config


class CatalogueStrategyProtocol(Protocol):
    def create(self, props):
        ...

    def create_external_volume(self, volume_source: metis_data.S3ExternalVolumeSource):
        ...

    def create_checkpoint_volume(self, checkpoint_volume: metis_data.DeltaCheckpoint):
        ...

    def drop(self):
        ...

    @property
    def namespace_name(self):
        ...

    @property
    def catalogue(self):
        ...

    @property
    def data_product_name(self):
        ...

    def fully_qualified_name(self, table_name):
        ...

    def checkpoint_name(self, ns=None):
        ...

    @property
    def data_product_root(self) -> str:
        ...

    def checkpoint_volume(self, ns) -> str:
        ...


class SparkCatalogueStrategy(CatalogueStrategyProtocol):
    """
    """

    def __init__(self, session, cfg):
        self.session = session
        self.cfg = cfg

    def maybe_sql(self, expr):
        return self.session.sql(expr)

    def create(self, props):
        (sql_builder.create_db(db_name=self.namespace_name,
                               db_property_expression=props)
         .maybe(None, self.maybe_sql))
        logger.info(f"{self.__class__.__name__}.create: {self.namespace_name}")
        return self

    def create_checkpoint_volume(self, checkpoint_root: metis_data.CheckpointVolumeRoot):
        """
        Where the checkpoint is in local, normally test, mode, the folder is created.
        No other strategy is supported aside from local create.
        """
        checkpoint_path = Path(checkpoint_root.path) / checkpoint_root.name
        logger.info(
            f"{self.__class__.__name__}.create_checkpoint_volume: {str(checkpoint_path)}")

        if not checkpoint_path.exists():
            checkpoint_path.mkdir(parents=True, exist_ok=True)
        return self

    def create_external_volume(self, volume_source: metis_data.S3ExternalVolumeSource):
        """
        External volumes are only created on Databricks, so this is a noop
        """
        logger.info(
            f"{self.__class__.__name__}.create_external_volume: {self.fully_qualified_volume_name(volume_source.name)} {volume_source.location}")
        return self

    def drop(self):
        self.session.sql(f"drop database IF EXISTS {self.namespace_name} CASCADE")
        return self

    @property
    def namespace_name(self):
        return self.cfg.data_product

    @property
    def catalogue(self):
        return self.cfg.catalogue

    @property
    def data_product_name(self):
        return self.cfg.data_product

    def fully_qualified_name(self, table_name):
        return f"{self.data_product_root}.{table_name}"

    def fully_qualified_volume_name(self, volume_name):
        return f"{self.data_product_root}.{volume_name}"

    def checkpoint_name(self, ns=None):
        return self.cfg.checkpoint_name

    @property
    def data_product_root(self) -> str:
        return self.namespace_name

    def checkpoint_volume(self, root: metis_data.CheckpointVolumeRoot, name: str) -> str:
        return f"/Volumes/{self.catalogue}/{self.namespace_name}/{root.name}/{name}"


class UnityCatalogueStrategy(CatalogueStrategyProtocol):

    def __init__(self, session, cfg: config.Config):
        self.session = session
        self.cfg = cfg

    def maybe_sql(self, expr):
        return self.session.sql(expr)

    def create(self, props):
        (sql_builder.create_db(db_name=self.fully_qualified_schema_name(),
                               db_property_expression=props)
         .maybe(None, partial(logger.maybe_debug, f"{self.__class__.__name__}.create"))
         .maybe(None, self.maybe_sql))
        if self.cfg.owner:
            self.set_schema_owner(self.cfg.owner)
        return self

    def set_schema_owner(self, owner):
        return (sql_builder.set_owner_of_schema(db_name=self.fully_qualified_schema_name(),
                                                owner=owner)
                .maybe(None, partial(logger.maybe_debug, f"{self.__class__.__name__}.set_schema_owner"))
                .maybe(None, self.maybe_sql))

    def create_checkpoint_volume(self, checkpoint_volume: metis_data.DeltaCheckpoint):
        (sql_builder.create_managed_volume(self.fully_qualified_checkpoint_volume(checkpoint_volume.name))
         .maybe(None, partial(logger.maybe_debug, f"{self.__class__.__name__}.create_checkpoint_volume"))
         .maybe(None, self.maybe_sql))

        self.set_volume_owner(volume=self.fully_qualified_volume_name(checkpoint_volume.name),
                              owner=self.cfg.owner)

        return self

    def create_external_volume(self, volume_source: metis_data.S3ExternalVolumeSource):
        """
        External volumes are only created on Databricks
        """
        (sql_builder.create_external_volume(self.fully_qualified_volume_name(volume_source.name),
                                            volume_source.external_bucket)
         .maybe(None, partial(logger.maybe_debug, f"{self.__class__.__name__}.create_external_volume"))
         .maybe(None, self.maybe_sql))

        self.set_volume_owner(volume=self.fully_qualified_volume_name(volume_source.name),
                              owner=self.cfg.owner)
        return self

    def set_volume_owner(self, volume, owner):
        return (sql_builder.set_owner_of_volume(volume=volume,
                                                owner=owner)
                .maybe(None, partial(logger.maybe_debug, f"{self.__class__.__name__}.set_volume_owner"))
                .maybe(None, self.maybe_sql))

    def drop(self):
        self.session.sql(f"drop database IF EXISTS {self.namespace_name} CASCADE")
        return self

    @property
    def namespace_name(self):
        return self.cfg.data_product

    @property
    def catalogue(self):
        return self.cfg.catalogue

    @property
    def data_product_name(self):
        return self.cfg.data_product

    def checkpoint_name(self, ns=None):
        return self.cfg.checkpoint_name

    def fully_qualified_name(self, table_name):
        return f"{self.data_product_root}.{table_name}"

    def fully_qualified_volume_name(self, volume_name):
        return f"{self.data_product_root}.{volume_name}"

    def fully_qualified_checkpoint_volume(self, name):
        return f"{self.data_product_root}.{name}"

    def fully_qualified_schema_name(self):
        return f"{self.catalogue}.{self.namespace_name}"

    @property
    def data_product_root(self) -> str:
        return f"{self.catalogue}.{self.namespace_name}"

    def checkpoint_volume(self, root: metis_data.CheckpointVolumeRoot, name: str) -> str:
        return f"/Volumes/{self.catalogue}/{self.namespace_name}/{root.name}/{name}"


class NameSpace:
    """
    A namespace is the logical location of the domain's artefacts, specifically tables (or schemas)
    and volumes.  A Namespace behaviour differs between local PySpark environments and a Databricks
    Unity-based environment.
    In the Unity world, we have 3 main layers, Catalogue, Schemas (or Databases) and Tables.  Keeping
    the local PySpark (and Delta) env as close as possible requires that the namespace has 2 layers,
    Catalogue and Table.
    """

    def __init__(self,
                 session,
                 cfg: config.Config):
        self.session = session
        self.cfg = cfg
        self.catalogue_strategy = self.determine_naming_convention()
        self.create_schema_if_not_exists()
        self.create_checkpoint_root_volume()

    def determine_naming_convention(self):
        if self.cfg.namespace_strategy_cls:
            return self.cfg.namespace_strategy_cls(self.session, self.cfg)
        match self.cfg.catalogue_mode:
            case config.CatalogueMode.SPARK:
                return SparkCatalogueStrategy(self.session, self.cfg)
            case config.CatalogueMode.UNITY:
                return UnityCatalogueStrategy(self.session, self.cfg)
            case _:
                raise error.generate_error(error.ConfigurationError, ("namespace", 1))

    #
    # DB LifeCycle Functions
    #
    def create_schema_if_not_exists(self):
        if not self.namespace_exists():
            self.catalogue_strategy.create(props=self.property_expr())

    def drop(self):
        self.catalogue_strategy.drop()
        return self

    def create_external_volume(self, volume_source: metis_data.S3ExternalVolumeSource):
        self.catalogue_strategy.create_external_volume(volume_source)
        return self

    def create_checkpoint_root_volume(self):
        self.catalogue_strategy.create_checkpoint_volume(self.cfg.checkpoint_volume)
        return self

    def fully_qualified_table_name(self, table_name):
        return self.catalogue_strategy.fully_qualified_name(table_name)

    def namespace_exists(self) -> bool:
        return self.session.catalog.databaseExists(self.catalogue_strategy.namespace_name)

    def table_exists(self, table_name):
        return table_name in self.list_tables()

    def catalog_table_exists(self, table_name):
        return self.session.catalog.tableExists(table_name)

    def list_tables(self):
        return [table.name for table in self.session.catalog.listTables(self.catalogue_strategy.data_product_root)]

    def table_format(self):
        return self.cfg.db.table_format

    #
    # DB Property Functions
    #
    def asserted_properties(self):
        return self.__class__.db_properties if hasattr(self, 'db_properties') else None

    def property_expr(self):
        return properties.DbProperty.property_expression(self.asserted_properties())
