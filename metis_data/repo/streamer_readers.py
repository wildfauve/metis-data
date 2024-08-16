from __future__ import annotations

from enum import Enum
from functools import partial, reduce
from typing import Optional
from pyspark.sql import dataframe, types as T

import metis_data
from . import spark_util
from metis_data.util import fn


class ReaderSwitch(Enum):
    READ_STREAM_WITH_SCHEMA_ON = ('read_stream_with_schema', True)  # Read a stream with a schema applied.
    READ_STREAM_WITH_SCHEMA_OFF = ('read_stream_with_schema', False)  # with no schema applied.

    GENERATE_DF_ON = ("generate_df", True)  # for Delta Table reads, return a DF rather than the delta table object
    GENERATE_DF_OFF = ("generate_df", False)  # return a delta table object

    @classmethod
    def merge_options(cls, defaults: Optional[set], overrides: Optional[set] = None) -> set[ReaderSwitch]:
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


class DeltaStreamReader:

    def __init__(self, spark_options: list[spark_util.SparkOption] = None):
        self.spark_options = spark_options if spark_options else []

    def read(self,
             table: metis_data.DomainTable,
             reader_options: Optional[set[ReaderSwitch]] = None) -> Optional[dataframe.DataFrame]:
        opts = set() if not reader_options else reader_options
        return self._read_stream(table.spark_session,
                                 table.fully_qualified_table_name(),
                                 ReaderSwitch.READ_STREAM_WITH_SCHEMA_ON in opts,
                                 table.schema)

    def _read_stream(self,
                     session,
                     table_name: str,
                     with_schema: bool,
                     read_schema: T.StructType):
        if with_schema and read_schema:
            return (session
                    .readStream
                    .schema(read_schema)
                    .format('delta')
                    .option('ignoreChanges', True)
                    .table(table_name))

        return (session
                .readStream
                .format('delta')
                .option('ignoreChanges', True)
                .table(table_name))



class SparkRecursiveFileStreamer:
    default_spark_options = [spark_util.SparkOption.RECURSIVE_LOOKUP]

    def __init__(self, spark_options: list[spark_util.SparkOption] = None):
        self.spark_options = spark_options if spark_options else []

    def read_stream(self,
                    cloud_file: metis_data.CloudFiles,):
        return (cloud_file.spark_session
                .readStream
                .options(**self._spark_opts())
                .schema(cloud_file.schema)
                .json(cloud_file.cloud_source.location, multiLine=True, prefersDecimal=True))

    def _spark_opts(self):
        return metis_data.SparkOption.function_based_options(self.__class__.default_spark_options + self.spark_options)


class DatabricksCloudFilesStreamer:
    """
    To use this stream the code must be running on a Databricks cluster.
    It returns a dataframe in streaming mode, using this pipeline...

    (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", <a-volume-location-for-checkpoints>)
    .load(<the-managed-or-external-volume-folder-containing-the-event>)

    To configure the stream:
    > opts = [metis_data.SparkOptions.JSON_CLOUD_FILES_FORMAT]  # only JSON is supported.
    > DatabricksCloudFilesStreamer(spark_options=opts)
    """
    format = "cloudFiles"
    default_spark_options = []

    def __init__(self, spark_options: list[spark_util.SparkOption] = None):
        self.spark_options = spark_options if spark_options else []


    def read_stream(self,
                    cloud_file: metis_data.CloudFiles):
        return (cloud_file.spark_session
                .readStream
                .format(self.__class__.format)
                .options(**self._spark_opts(cloud_file))
                .schema(cloud_file.schema)
                .load(cloud_file.cloud_source.location))

    def _spark_opts(self, cloud_file):
        opts = spark_util.SparkOption.function_based_options(self.__class__.default_spark_options + self.spark_options)
        return {**opts,
                **{'cloudFiles.schemaLocation': cloud_file.checkpoint_location}}
