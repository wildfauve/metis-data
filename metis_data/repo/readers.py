from __future__ import annotations
from typing import Dict, Protocol, Set
from functools import partial, reduce

from enum import Enum

from pyspark.sql import types as T
from pyspark.sql import dataframe
from delta.tables import *

from metis_job.util import fn, monad


class ReaderSwitch(Enum):
    READ_STREAM_WITH_SCHEMA_ON = ('read_stream_with_schema', True)  # Read a stream with a schema applied.
    READ_STREAM_WITH_SCHEMA_OFF = ('read_stream_with_schema', False)  # with no schema applied.

    GENERATE_DF_ON = ("generate_df", True)  # for Delta Table reads, return a DF rather than the delta table object
    GENERATE_DF_OFF = ("generate_df", False)  # return a delta table object

    @classmethod
    def merge_options(cls, defaults: Optional[Set], overrides: Optional[Set] = None) -> Set[ReaderSwitch]:
        if overrides is None:
            return defaults

        return reduce(cls.merge_switch, overrides, defaults)

    @classmethod
    def merge_switch(cls, options, override):
        default_with_override = fn.find(partial(cls.option_predicate, override.value[0]), options)
        if not default_with_override:
            options.add(override)
            return options
        options.remove(default_with_override)
        options.add(override)
        return options

    @classmethod
    def option_predicate(cls, option_name, option):
        return option_name == option.value[0]


class ReaderProtocol(Protocol):

    def read(self,
             repo,
             reader_options: Optional[Set[ReaderSwitch]]) -> Optional[dataframe.DataFrame]:
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

    default_reader_options = {ReaderSwitch.GENERATE_DF_ON}

    def read(self,
             repo,
             reader_options: set[ReaderSwitch] = None) -> dataframe.DataFrame | DeltaTable:
        if not repo.table_exists():
            return None

        if ReaderSwitch.GENERATE_DF_ON in self._merged_options(reader_options):
            return self._table_as_df(repo)

        if ReaderSwitch.GENERATE_DF_OFF in self._merged_options(reader_options):
            return self._table_as_delta_table(repo)

        return self._table_as_df(repo)

    #
    def _table_as_df(self, repo) -> DeltaTable:
        return repo.namespace.session.table(repo.fully_qualified_table_name())

    def _table_as_delta_table(self, repo) -> DeltaTable:
        return DeltaTable.forName(repo.namespace.session, repo.fully_qualified_table_name())


    def _merged_options(self, passed_reader_options: Set[ReaderSwitch] = None) -> Set[ReaderSwitch]:
        if not isinstance(passed_reader_options, set):
            return self.__class__.default_reader_options
        return ReaderSwitch.merge_options(self.__class__.default_reader_options.copy(),
                                          passed_reader_options)
