from __future__ import annotations
import metis_data
from metis_data.repo import spark_util


class SparkStreamingTableWriter:
    default_stream_trigger_condition = {'availableNow': True}

    def __init__(self, spark_options: list[spark_util.SparkOption] = None):
        self.spark_options = spark_options if spark_options else []


    def write_stream(self,
                     streaming_df,
                     stream_reader):
        opts = {**spark_util.SparkOption.function_based_options(self.spark_options if self.spark_options else []),
                **{'checkpointLocation': stream_reader.checkpoint_location}}
        streaming_query = (streaming_df
                           .writeStream
                           .options(**opts)
                           .trigger(**self.__class__.default_stream_trigger_condition)
                           .toTable(stream_reader.stream_to_table_name))
        streaming_query.awaitTermination()
        return streaming_query


class DeltaStreamingTableWriter:
    format = "delta"
    default_stream_trigger_condition = {'availableNow': True}

    def __init__(self, spark_options: list[spark_util.SparkOption] = None):
        self.spark_options = spark_options if spark_options else []

    def write_stream(self,
                     streaming_df,
                     stream_reader):
        return self._write_stream_append_only(streaming_df, stream_reader)

    def _write_stream_append_only(self,
                                  streaming_df,
                                  stream_reader):
        opts = {**spark_util.SparkOption.function_based_options(self.spark_options if self.spark_options else []),
                **{'checkpointLocation': stream_reader.checkpoint_location}}
        streaming_query = (streaming_df.writeStream
                           .format(self.__class__.format)
                           .outputMode("append")
                           .options(**opts)
                           .trigger(**self.__class__.default_stream_trigger_condition)
                           .toTable(stream_reader.to_table_name()))
        streaming_query.awaitTermination()
        return streaming_query
