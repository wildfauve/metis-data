from __future__ import annotations

from pyspark.sql import dataframe, types
from delta.tables import *

from metis_data import repo, DomainTable, namespace as ns

from .util import error, monad

CloudFilesStreamReader = repo.SparkRecursiveFileStreamer | repo.DatabricksCloudFilesStreamer
CloudFilesStreamWriter = repo.SparkStreamingTableWriter | repo.DeltaStreamingTableWriter


class CloudFiles:

    def __init__(self,
                 spark_session: SparkSession,
                 namespace: ns.NameSpace,
                 stream_reader: CloudFilesStreamReader,
                 cloud_source: str,
                 schema: types.StructType,
                 stream_writer: CloudFilesStreamWriter,
                 stream_to_table_name: str = None,
                 stream_to_table: DomainTable = None):
        self.spark_session = spark_session
        self.namespace = namespace
        self.stream_reader = stream_reader
        self.cloud_source = cloud_source
        self.schema = schema
        self.stream_writer = stream_writer
        self.stream_to_table_name = stream_to_table_name
        self.stream_to_table = stream_to_table

    @monad.Try(error_cls=error.CloudFilesStreamingError)
    def try_read_stream(self) -> dataframe.DataFrame:
        return self.read_stream()

    def read_stream(self) -> dataframe.DataFrame:
        return self.stream_reader.read_stream(self)

    def write_stream(self, df) -> dataframe.DataFrame:
        return self.stream_writer.write_stream(df, self)

    def to_table_name(self):
        # Either a DomainTable or a table_name as a string
        if self.stream_to_table_name:
            return self.stream_to_table_name
        return self.stream_to_table.fully_qualified_table_name()

    @property
    def checkpoint_location(self):
        return self.namespace.catalogue_strategy.checkpoint_volume
