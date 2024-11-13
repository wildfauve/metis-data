from __future__ import annotations

from typing import Optional

from metis_data import const
from metis_data.repo import spark_util
from metis_data.util import logger

logging_ns = f"{const.NS}.streamWriters"

class SparkStreamingTableWriter:
    default_stream_trigger_condition = {'availableNow': True}

    def __init__(self,
                 spark_options: list[spark_util.SparkOption] = None,
                 trigger_condition: dict = None):
        self.spark_options = spark_options if spark_options else []
        self.trigger_condition = trigger_condition if trigger_condition else self.__class__.default_stream_trigger_condition


    def write_stream(self,
                     streaming_df,
                     stream_coordinator,
                     trigger_condition: dict = None):
        opts = {**spark_util.SparkOption.function_based_options(self.spark_options if self.spark_options else []),
                **{'checkpointLocation': stream_coordinator.checkpoint_location}}

        logger.debug(f"{logging_ns}.sparkStreamingTableWriter.write_stream opts {str(opts)} toTable {stream_coordinator.stream_to_table_name}")
        streaming_query = (streaming_df
                           .writeStream
                           .options(**opts)
                           .trigger(**self.write_trigger(trigger_condition))
                           .toTable(stream_coordinator.stream_to_table_name))
        streaming_query.awaitTermination()
        return streaming_query

    def write_trigger(self, write_stream_trigger):
        if write_stream_trigger:
            return write_stream_trigger
        return self.trigger_condition


class DeltaStreamingTableWriter:
    format = "delta"
    default_stream_trigger_condition = {'availableNow': True}

    def __init__(self,
                 spark_options: list[spark_util.SparkOption] = None,
                 trigger_condition: dict = None):
        self.spark_options = spark_options if spark_options else []
        self.trigger_condition = trigger_condition if trigger_condition else self.__class__.default_stream_trigger_condition

    def write_stream(self,
                     streaming_df,
                     stream_coordinator,
                     trigger_condition: dict = None,
                     spark_options: Optional[list[spark_util.SparkOption]] = None):
        """
        The Stream Coordinator is either A CloudFiles instance or a Domain Table instance.  When streaming from an
        external file source, it will be a CloudFile.  When streaming from a Delta table, it will be a DomainTable.
        """
        return self._write_stream_append_only(streaming_df, stream_coordinator, trigger_condition)

    def _write_stream_append_only(self,
                                  streaming_df,
                                  stream_coordinator,
                                  trigger_condition):
        opts = {**spark_util.SparkOption.function_based_options(self.spark_options if self.spark_options else []),
                **{'checkpointLocation': stream_coordinator.checkpoint_location}}

        logger.debug(f"{logging_ns}.deltaStreamingTableWriter.write_stream opts {str(opts)} toTable {stream_coordinator.to_table_name()}")

        streaming_query = (streaming_df.writeStream
                           .format(self.__class__.format)
                           .outputMode("append")
                           .options(**opts)
                           .trigger(**self.write_trigger(trigger_condition))
                           .toTable(stream_coordinator.to_table_name()))
        streaming_query.awaitTermination()
        return streaming_query

    def write_trigger(self, write_stream_trigger):
        if write_stream_trigger:
            return write_stream_trigger
        return self.trigger_condition
