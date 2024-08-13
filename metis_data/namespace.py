from __future__ import annotations

from typing import Protocol

import metis_data
from metis_data.util import logger, error
from metis_data.repo import sql_builder, properties
from . import config


class CatalogueStrategyProtocol(Protocol):
    def create(self, props, if_not_exists=True):
        ...

    def create_external_volume(self, volume_source: metis_data.S3ExternalVolumeSource):
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

    @property
    def checkpoint_name(self):
        ...

    @property
    def data_product_root(self) -> str:
        ...

    @property
    def checkpoint_volume(self) -> str:
        ...


class SparkCatalogueStrategy(CatalogueStrategyProtocol):
    """
    """

    def __init__(self, session, cfg):
        self.session = session
        self.cfg = cfg

    def maybe_sql(self, expr):
        return self.session.sql(expr)

    def create(self, props, if_not_exists=True):
        (sql_builder.create_db(db_name=self.namespace_name,
                               db_property_expression=props)
         .maybe(None, self.maybe_sql))
        return self

    def create_external_volume(self, volume_source: metis_data.S3ExternalVolumeSource):
        """
        External volumes are only created on Databricks, so this is a noop
        """
        # (sql_builder.create_external_volume(self.fully_qualified_volume_name(volume_source.name),
        #                                     volume_source.location)
        #  .maybe(self.maybe_sql))
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

    @property
    def checkpoint_name(self):
        return self.cfg.checkpoint_name

    @property
    def data_product_root(self) -> str:
        return self.namespace_name

    @property
    def checkpoint_volume(self) -> str:
        return f"/Volumes/{self.catalogue}/{self.namespace_name}/checkpoints/{self.checkpoint_name}"


class UnityCatalogueStrategy(CatalogueStrategyProtocol):
    def __init__(self, session, cfg: config.Config):
        self.session = session
        self.cfg = cfg

    def maybe_sql(self, expr):
        return self.session.sql(expr)

    def create(self, props, if_not_exists=True):
        (sql_builder.create_db(db_name=self.namespace_name,
                               db_property_expression=props)
         .maybe(None, self.maybe_sql))
        return self

    def create_external_volume(self, volume_source: metis_data.S3ExternalVolumeSource):
        """
        External volumes are only created on Databricks
        """
        (sql_builder.create_external_volume(self.fully_qualified_volume_name(volume_source.name),
                                            volume_source.location)
         .maybe(self.maybe_sql))
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

    @property
    def checkpoint_name(self):
        return self.cfg.checkpoint_name

    def fully_qualified_name(self, table_name):
        return f"{self.data_product_root}.{table_name}"

    def fully_qualified_volume_name(self, volume_name):
        return f"{self.data_product_root}.{volume_name}"

    @property
    def data_product_root(self) -> str:
        return f"{self.catalogue}.{self.namespace_name}"

    @property
    def checkpoint_volume(self) -> str:
        return f"/Volumes/{self.catalogue}/{self.namespace_name}/checkpoints/{self.checkpoint_name}"


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
        self.create_if_not_exists()

    def determine_naming_convention(self):
        if self.cfg.namespace_strategy_cls:
            return self.cfg.namespace_strategy_cls(self.session, self.cfg)
        match self.cfg.catalogue_mode:
            case config.CatalogueMode.SPARK:
                return SparkCatalogueStrategy(self.session, self.cfg)
            case config.CatalogueMode.UNITY:
                return UnityCatalogueStrategy(self.session, self.cfg)
            case _:
                raise error.generate_error(error.ConfigurationError, (422, 1))

    #
    # DB LifeCycle Functions
    #
    def create_if_not_exists(self):
        self.catalogue_strategy.create(props=self.property_expr(), if_not_exists=True)

    def drop(self):
        self.catalogue_strategy.drop()
        return self

    def create_external_volume(self, volume_source: metis_data.S3ExternalVolumeSource):
        self.catalogue_strategy.create_external_volume(volume_source)
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
