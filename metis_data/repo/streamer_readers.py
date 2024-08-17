from __future__ import annotations

from enum import Enum
from functools import partial, reduce
from typing import Optional
from pyspark.sql import dataframe, types as T

import metis_data
from . import spark_util, reader_options




class DeltaStreamReader:

    def __init__(self, spark_options: list[spark_util.SparkOption] = None):
        self.spark_options = spark_options if spark_options else []

    def read(self,
             table: metis_data.DomainTable,
             reader_opts: Optional[set[reader_options.ReaderSwitch]] = None) -> Optional[dataframe.DataFrame]:
        opts = set() if not reader_opts else reader_opts
        return self._read_stream(table.spark_session,
                                 table.fully_qualified_table_name(),
                                 reader_options.ReaderSwitch.READ_STREAM_WITH_SCHEMA_ON in opts,
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
                    cloud_file: metis_data.CloudFiles,
                    reader_options: Optional[set[reader_options.ReaderSwitch]] = None):
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
                    cloud_file: metis_data.CloudFiles,
                    reader_options: Optional[set[reader_options.ReaderSwitch]] = None):
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
