from __future__ import annotations
from typing import Protocol, Set, Callable

from pyspark.sql import dataframe
from delta.tables import *

from metis_data.util import error, monad

from . import spark_util


class WriterProtocol(Protocol):

    def try_write_append(self,
                         repo,
                         df: dataframe.DataFrame,
                         table_name: str) -> monad.Either[error.RepoWriteError, dataframe.DataFrame]:
        """
        """
        ...


class DeltaTableWriter(WriterProtocol):
    """
    """

    @monad.Try(error_cls=error.RepoWriteError)
    def try_write_append(self, table, df, options: Optional[List[spark_util.SparkOption]] = []):
        return self._write_append(table, df, options)

    @monad.Try(error_cls=error.RepoWriteError)
    def try_upsert(self,
                   table,
                   delta_table: DeltaTable,
                   df,
                   options: Optional[List[spark_util.SparkOption]] = []):
        """
        Table must be a DeltaTable object.
        :param table:
        :param df:
        :param options:
        :return:
        """
        return self._perform_upsert(table, delta_table, df, options)

    def _write_append(self, table, df, options: Optional[List[spark_util.SparkOption]] = []):
        """
        Executes a simple append operation on a table using the provided dataframe.
        + Optionally provide a tuple of columns for partitioning the table.
        """
        result = (df.write
                  .format(table.table_format)
                  .partitionBy(table.partition_on())
                  .options(**spark_util.SparkOption.function_based_options(options))
                  .mode("append")
                  .saveAsTable(table.fully_qualified_table_name()))
        return result

    def _perform_upsert(self,
                        table,
                        delta_table: DeltaTable,
                        df,
                        _batch_id=None,
                        options: Optional[List[spark_util.SparkOption]] = []):
        # session.set_session_config_options(self.db.session, spark_util.SparkOption.options_to_spark_options(options))

        # dtable = self.delta_table(reader_options={readers.ReaderSwitch.READ_STREAM_WITH_SCHEMA_OFF,
        #                                           readers.ReaderSwitch.GENERATE_DF_OFF})
        #
        # if not dtable:
        #     raise repo_messages.delta_table_not_returned()
        upserter = (delta_table.alias(table.table_name)
                    .merge(df.alias('updates'),
                           self.build_merge_condition(table.table_name,
                                                      'updates',
                                                      table.merge_condition(),
                                                      table.prune_on()))
                    .whenNotMatchedInsertAll())
        # Note that the conditions appear to return a new version of the upserter, rather than mutating the merge
        # class.  Therefore, to apply the UpdateAll and Delete conditionally, the upserter var needs to be updated with
        # the new version of the merge class.
        # TODO: a better approach might be a reduce(condition_applier, [whenMatchedUpdateAll, whenMatchedDelete], upserter)
        if hasattr(table, 'update_condition'):
            upserter = upserter.whenMatchedUpdateAll(condition=table.update_condition(table.table_name, 'updates'))
        else:
            upserter = upserter.whenMatchedUpdateAll()

        if hasattr(table, 'delete_condition'):
            upserter = upserter.whenMatchedDelete(condition=table.delete_condition(self.table_name, 'updates'))

        return upserter.execute()

    def build_merge_condition(self, name_of_baseline,
                              update_name,
                              merge_conditions: Callable,
                              partition_pruning_col):
        if not merge_conditions:
            raise error.generate_error(error.RepoConfigurationError, ("writers", 3))

        pruning_cond = self._build_puning_condition(name_of_baseline, update_name, partition_pruning_col)

        identity_cond = merge_conditions(name_of_baseline, update_name)

        return f"{pruning_cond}{identity_cond}"

    def _build_puning_condition(self, name_of_baseline, update_name, partition_puning_col):
        if partition_puning_col:
            return f"{name_of_baseline}.{partition_puning_col} = {update_name}.{partition_puning_col} AND "
        return ""
