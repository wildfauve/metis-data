from __future__ import annotations

from pyspark.sql import dataframe
from delta.tables import *

from metis_job import repo
from . import namespace as ns
from .util import error, monad

ReaderType = repo.DeltaTableReader
WriterType = repo.DeltaTableWriter


class CreateManagedDeltaTable:
    def perform(self, table: DomainTable):
        self._create(table.namespace.session,
                     table.schema,
                     table.fully_qualified_table_name(),
                     table.partition_on(),
                     table.asserted_table_properties())
        table.property_manager.invalidate_property_cache()
        pass

    def _create(self,
                spark_session,
                schema,
                fully_qualified_table_name: str,
                partition_cols,
                table_props: list[repo.TableProperty]):

        """
        See https://docs.delta.io/latest/api/python/spark/index.html#delta.tables.DeltaTableBuilder
        :param spark_session:
        :param schema:
        :param fully_qualified_table_name:
        :param partition_cols:
        :param table_props:
        :return:
        """
        builder = (DeltaTable.createIfNotExists(spark_session)
                   .tableName(fully_qualified_table_name)
                   .addColumns(schema))
        if partition_cols:
            builder = builder.partitionedBy(partition_cols)
        if table_props:
            for prop in table_props:
                builder = builder.property(prop.key, prop.value)
        result = builder.execute()
        return result


class DomainTable:
    schema = None
    table_name = None
    partition_columns = None
    table_properties = None
    table_format = "delta"

    def __init__(self,
                 namespace: ns.NameSpace = None,
                 reader: ReaderType = repo.DeltaTableReader,
                 writer: WriterType = repo.DeltaTableWriter,
                 table_creation_protocol=CreateManagedDeltaTable):
        self.namespace = namespace
        self.reader = reader
        self.writer = writer
        self.table_creation_protocol = table_creation_protocol

        self.property_manager = repo.properties.TablePropertyManager(
            session=self.namespace.session,
            asserted_properties=self.asserted_table_properties(),
            fully_qualified_table_name=self.fully_qualified_table_name())

        self.properties = self.property_manager  # hides, a little, the class managing properties.
        self.after_initialise()  # callback Hook

    def read(self, reader_options=None) -> dataframe.DataFrame:
        return self.reader().read(self, reader_options=reader_options)

    def table_exists(self) -> bool:
        return self.namespace.table_exists(self.__class__.table_name)

    def fully_qualified_table_name(self):
        return self.namespace.fully_qualified_table_name(self.table_name)

    def partition_on(self):
        return self.__class__.partition_columns if hasattr(self, 'partition_columns') else tuple()

    def prune_on(self):
        return self.__class__.pruning_column if hasattr(self, 'pruning_column') else None

    def merge_condition(self):
        return self.identity_merge_condition if hasattr(self, 'identity_merge_condition') else None

    def perform_table_creation_protocol(self):
        if not self.table_creation_protocol and not self.__class__.table_creation_protocol:
            raise error.generate_error(error.RepoConfigurationError, (422, 2))
        if self.table_creation_protocol:
            self.table_creation_protocol().perform(self)
        else:
            self.__class__.table_creation_protocol().perform(self)
        return self

    def table_property_expr(self):
        return repo.TableProperty.table_property_expression(self.asserted_table_properties())

    def asserted_table_properties(self):
        return self.__class__.table_properties if hasattr(self, 'table_properties') else None

    #
    # Table Write Functions
    #
    def create_df(self, data, schema=None):
        return self.db.session.createDataFrame(data=data,
                                               schema=self.determine_schema_to_use_for_df(schema))

    def try_write_append(self, df, options: Optional[List[repo.SparkOption]] = []):
        result = self.writer().try_write_append(self, df, options)
        self.after_append(result)
        return result

    def try_upsert(self, df, options: Optional[List[repo.SparkOption]] = []):
        """
        The try_upsert wraps the upsert function with a Try monad.  The result will be an Either.  A successful result
        usually returns Right(None).
        """
        if not self.table_exists():
            return self.try_write_append(df)

        result = self.writer().try_upsert(self,
                                          self.read(reader_options={repo.ReaderSwitch.GENERATE_DF_OFF}),
                                          df,
                                          options)

        self.after_upsert()  # callback hook.

        return result

    # Abstract Callbacks
    def after_initialise(self):
        ...

    def after_append(self, _result):
        ...

    def after_upsert(self):
        ...
