from __future__ import annotations
from typing import Protocol

from pyspark.sql import dataframe
from delta.tables import *

from metis_data.repo import spark_util, reader_options


class ReaderProtocol(Protocol):

    def read(self,
             repo,
             reader_opts: Optional[set[reader_options.ReaderSwitch]]) -> Optional[dataframe.DataFrame]:
        """
        Takes a repository object SparkRepo, and an optional table name and performs a read operation, returning a
        DataFrame.  The result may be optional, especially in the case where the table has yet to be created or
        is not found in the catalogue.
        """
        ...


class DeltaTableReader(ReaderProtocol):
    """
    Delta reader using the DeltaTable class
    """
    default_reader_options = {reader_options.ReaderSwitch.GENERATE_DF_ON}

    def __init__(self, spark_options: list[spark_util.SparkOption] = None):
        self.spark_options = spark_options if spark_options else []

    def read(self,
             repo,
             reader_opts: set[reader_options.ReaderSwitch] = None) -> dataframe.DataFrame | DeltaTable:
        if not repo.table_exists():
            return None

        if reader_options.ReaderSwitch.GENERATE_DF_ON in self._merged_options(reader_opts):
            return self._table_as_df(repo)

        if reader_options.ReaderSwitch.GENERATE_DF_OFF in self._merged_options(reader_opts):
            return self._table_as_delta_table(repo)

        return self._table_as_df(repo)

    #
    def _table_as_df(self, repo) -> DeltaTable:
        return repo.namespace.session.table(repo.fully_qualified_table_name())

    def _table_as_delta_table(self, repo) -> DeltaTable:
        return DeltaTable.forName(repo.namespace.session, repo.fully_qualified_table_name())

    def _merged_options(self,
                        passed_reader_opts: set[reader_options.ReaderSwitch] = None) -> set[
        reader_options.ReaderSwitch]:
        if not isinstance(passed_reader_opts, set):
            return self.__class__.default_reader_options
        return reader_options.ReaderSwitch.merge_options(self.__class__.default_reader_options.copy(),
                                                         passed_reader_opts)
