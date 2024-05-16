from __future__ import annotations

from typing import Protocol
from metis_data.util import logger, error
from metis_data.repo import sql_builder, properties
from . import config


class NamingConventionProtocol(Protocol):

    def namespace_name(self) -> str:
        """
        The database name is provide in the dbconfig section of the runner config.  This function returns that name.
        :return:
        """
        ...

    def table_name(self, table_name) -> str:
        """
        This function combines the database name and the provided table name.

        Used when using hive-based operations; like drop table, or spark.table(db_table_name("t1")

        Used by:
        + HiveTableReader().read
        + HiveRepo().drop_table_by_name
        + HiveRepo().read_stream
        + HiveRepo().create
        + HiveRepo().get_table_properties
        + HiveRepo().add_to_table_properties
        + HiveRepo().remove_from_table_properties

        :param table_name:
        :return:
        """
        ...

    def catalogue(self) -> str:
        """
        """
        ...

    def data_product_name(self) -> str:
        """
        """
        ...

    def fully_qualified_name(self, table_name: str) -> str:
        """
        """
        ...

    def data_product_root(self) -> str:
        """
        Used by List tables.  Which is in turn used to determine if a table
        exists in a name space
        :return:
        """
        ...


class SparkNamingConventionDomainBased(NamingConventionProtocol):
    """
    """

    def __init__(self, job_config):
        self.config = job_config

    def namespace_name(self):
        return self.config.data_product

    def catalogue(self):
        return self.config.catalogue

    def data_product_name(self):
        return self.config.data_product

    def fully_qualified_name(self, table_name):
        return f"{self.namespace_name()}.{table_name}"

    def data_product_root(self) -> str:
        return self.namespace_name()


class UnityNamingConventionDomainBased(NamingConventionProtocol):
    """
    """

    def __init__(self, job_config):
        self.config = job_config

    def namespace_name(self):
        return self.config.data_product

    def catalogue(self):
        return self.config.catalogue

    def data_product_name(self):
        return self.config.data_product

    def fully_qualified_name(self, table_name):
        return f"{self.catalogue()}.{self.namespace_name()}.{table_name}"

    def data_product_root(self) -> str:
        return f"{self.catalogue()}.{self.namespace_name()}"


class NameSpace:

    def __init__(self,
                 session,
                 job_config):
        self.session = session
        self.config = job_config
        self.naming = self.determine_naming_convention()
        self.create_namespace_if_not_exists()

    def determine_naming_convention(self):
        match self.config.job_mode:
            case config.JobMode.SPARK:
                return SparkNamingConventionDomainBased(self.config)
            case config.JobMode.UNITY:
                return UnityNamingConventionDomainBased(self.config)
            case _:
                raise error.generate_error(error.ConfigurationError,
                                           (422, 1))

    #
    # DB LifeCycle Functions
    #
    def create_namespace_if_not_exists(self):
        self.session.sql(sql_builder.create_db(db_name=self.naming.namespace_name(),
                                               db_property_expression=self.property_expr()))

    def drop_namespace(self):
        self.session.sql(f"drop database IF EXISTS {self.naming.namespace_name()} CASCADE")
        return self

    def fully_qualified_table_name(self, table_name):
        return self.naming.fully_qualified_name(table_name)

    def namespace_exists(self) -> bool:
        return self.session.catalog.databaseExists(self.naming.namespace_name())

    def table_exists(self, table_name):
        return table_name in self.list_tables()

    def catalog_table_exists(self, table_name):
        return self.session.catalog.tableExists(table_name)

    def list_tables(self):
        return [table.name for table in self.session.catalog.listTables(self.naming.data_product_root())]

    def table_format(self):
        return self.config.db.table_format

    #
    # DB Property Functions
    #
    def asserted_properties(self):
        return self.__class__.db_properties if hasattr(self, 'db_properties') else None

    def property_expr(self):
        return properties.DbProperty.property_expression(self.asserted_properties())
