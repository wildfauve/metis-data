import shutil
from pathlib import Path
from pyspark.sql.functions import col, current_timestamp, lit

import metis_data
from metis_data.util import error

from tests.shared import spark_test_session, namespaces_and_tables, data

stream_source = "tests/spark_locations/stream_source"
checkpoint_name = "checkpoints"
checkpoint_loc = 'tests/spark_locations'

checkpoint_path = Path(checkpoint_loc) / checkpoint_name


def setup_function():
    if checkpoint_path.exists():
        shutil.rmtree(checkpoint_path)


def test_cloud_files_streaming(di_initialise_spark,
                               dataproduct1_ns):
    opts = [metis_data.SparkOption.MERGE_SCHEMA]

    sketches_table = namespaces_and_tables.my_table2_cls(streaming_table=True)(namespace=dataproduct1_ns)

    session = spark_test_session.spark_session()

    ext_vol = metis_data.S3ExternalVolumeSource(ns=dataproduct1_ns,
                                                name="events",
                                                source=stream_source,
                                                external_bucket="s3://bucket/folder")

    cloud_files = metis_data.CloudFiles(spark_session=session,
                                        namespace=dataproduct1_ns,
                                        stream_reader=metis_data.SparkRecursiveFileStreamer(),
                                        cloud_source=ext_vol,
                                        stream_to_table=sketches_table,
                                        schema=namespaces_and_tables.json_file_schema,
                                        stream_writer=metis_data.SparkStreamingTableWriter(opts),
                                        stream_to_table_name="dp1.sketch")

    df = cloud_files.read_stream()

    df2 = df.withColumns({'source_file': col("_metadata.file_path"),
                          'processing_time': current_timestamp()})

    result = cloud_files.write_stream(df2)

    sketch_df = spark_test_session.spark_session().read.table('dp1.sketch')

    assert sketch_df.count() == 4


def test_cloud_files_streaming_to_delta_append(di_initialise_spark,
                                               dataproduct1_ns):
    sketches_table = namespaces_and_tables.my_table2_cls(streaming_table=True)(namespace=dataproduct1_ns)

    ext_vol = metis_data.S3ExternalVolumeSource(ns=dataproduct1_ns,
                                                name="events",
                                                source=stream_source,
                                                external_bucket="s3://bucket/folder")

    cloud_files = metis_data.CloudFiles(spark_session=spark_test_session.spark_session(),
                                        namespace=dataproduct1_ns,
                                        stream_reader=metis_data.SparkRecursiveFileStreamer(),
                                        cloud_source=ext_vol,
                                        schema=namespaces_and_tables.json_file_schema,
                                        stream_writer=metis_data.DeltaStreamingTableWriter(),
                                        stream_to_table=sketches_table)
    df = cloud_files.read_stream()

    df2 = df.withColumns({'source_file': col("_metadata.file_path"),
                          'processing_time': current_timestamp()})

    result = cloud_files.write_stream(df2)

    sketch_df = sketches_table.read()

    assert sketch_df.count() == 4


def test_delta_table_streaming_flow(di_initialise_spark,
                                    dataproduct1_ns):
    opts = [metis_data.SparkOption.MERGE_SCHEMA]

    table_cls = namespaces_and_tables.my_table_cls()

    source_table = table_cls(namespace=dataproduct1_ns,
                             stream_reader=metis_data.DeltaStreamReader())

    stream_to_table = namespaces_and_tables.MyTable2(namespace=dataproduct1_ns,
                                                     stream_writer=metis_data.DeltaStreamingTableWriter(opts))
    source_table.try_write_append(data.my_table_df())

    df = source_table.read_stream()

    assert df.isStreaming

    df2 = df.withColumns({'onStream': lit("true")})

    stream_to_table.write_stream(df2)

    df3 = stream_to_table.read()

    assert [row.onStream for row in df3.select('onStream').collect()] == ['true', 'true']


def test_stream_using_streamer(di_initialise_spark,
                               dataproduct1_ns):
    def transform_fn(df):
        return df.withColumn('onStream', lit("true"))

    opts = [metis_data.SparkOption.MERGE_SCHEMA]

    table_cls = namespaces_and_tables.my_table_cls()

    source_table = table_cls(namespace=dataproduct1_ns,
                             stream_reader=metis_data.DeltaStreamReader())

    stream_to_table = namespaces_and_tables.MyTable2(namespace=dataproduct1_ns,
                                                     stream_writer=metis_data.DeltaStreamingTableWriter(opts))
    source_table.try_write_append(data.my_table_df())

    stream = (metis_data.Streamer()
              .stream_from(source_table,
                           stream_from_reader_options={metis_data.ReaderSwitch.READ_STREAM_WITH_SCHEMA_ON})
              .stream_to(table=stream_to_table,
                         write_type=metis_data.StreamWriteType.APPEND,
                         stream_trigger_condition={"availableNow": True})
              .with_transformer(transform_fn))

    result = stream.run()

    assert result.is_right()

    df = stream_to_table.read()

    assert [row.onStream for row in df.select('onStream').collect()] == ['true', 'true']


def test_stream_using_streamer_with_cloud_files_source(di_initialise_spark,
                                                       dataproduct1_ns):
    def transform_fn(df):
        return df.withColumn('onStream', lit("true"))

    opts = [metis_data.SparkOption.MERGE_SCHEMA]

    session = spark_test_session.spark_session()

    ext_vol = metis_data.S3ExternalVolumeSource(ns=dataproduct1_ns,
                                                name="events",
                                                source=stream_source,
                                                external_bucket="s3://bucket/folder")

    stream_to_table = namespaces_and_tables.MyTable2(namespace=dataproduct1_ns,
                                                     stream_writer=metis_data.DeltaStreamingTableWriter(opts))

    cloud_files = metis_data.CloudFiles(spark_session=session,
                                        namespace=dataproduct1_ns,
                                        stream_reader=metis_data.SparkRecursiveFileStreamer(),
                                        cloud_source=ext_vol,
                                        schema=namespaces_and_tables.json_file_schema,
                                        stream_to_table=stream_to_table)

    opts = [metis_data.SparkOption.MERGE_SCHEMA]

    stream = (metis_data.Streamer()
              .stream_from(cloud_files)
              .stream_to(table=stream_to_table,
                         write_type=metis_data.StreamWriteType.APPEND,
                         stream_trigger_condition={"availableNow": True})
              .with_transformer(transform_fn))

    result = stream.run()

    assert result.is_right()

    df = stream_to_table.read()

    assert [row.onStream for row in df.select('onStream').collect()] == ['true', 'true', 'true', 'true']


def test_stream_when_writer_throws_exception(di_initialise_spark,
                                             dataproduct1_ns):
    def transform_fn(df):
        return df.withColumn('onStream', lit("true"))

    class ErrorThrowingStreamWriter:
        def __init__(self,
                     spark_options=None,
                     trigger_condition: dict = None):
            pass

        def write_stream(self,
                         streaming_df,
                         stream_coordinator,
                         trigger_condition: dict = None,
                         spark_options=None):
            """
            The Stream Coordinator is either A CloudFiles instance or a Domain Table instance.  When streaming from an
            external file source, it will be a CloudFile.  When streaming from a Delta table, it will be a DomainTable.
            """
            raise Exception("BOOM!")

    opts = [metis_data.SparkOption.MERGE_SCHEMA]

    table_cls = namespaces_and_tables.my_table_cls()

    source_table = table_cls(namespace=dataproduct1_ns,
                             stream_reader=metis_data.DeltaStreamReader())

    stream_to_table = namespaces_and_tables.MyTable2(namespace=dataproduct1_ns,
                                                     stream_writer=ErrorThrowingStreamWriter(opts))
    source_table.try_write_append(data.my_table_df())

    stream = (metis_data.Streamer()
              .stream_from(source_table,
                           stream_from_reader_options={metis_data.ReaderSwitch.READ_STREAM_WITH_SCHEMA_ON})
              .stream_to(table=stream_to_table,
                         write_type=metis_data.StreamWriteType.APPEND,
                         stream_trigger_condition={"availableNow": True})
              .with_transformer(transform_fn))

    result = stream.run()

    assert result.is_left()
    err = result.error().exception
    assert isinstance(err, error.StreamerWriterError)
    assert err.message == "Stream Writer Error"
    assert err.ctx.get('cause') == "BOOM!"

