import shutil
from pathlib import Path
from pyspark.sql.functions import col, current_timestamp, lit

import metis_data

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

    session = spark_test_session.spark_session()

    ext_vol = metis_data.S3ExternalVolumeSource(ns=dataproduct1_ns,
                                                name="events",
                                                source=stream_source,
                                                external_bucket="s3://bucket/folder")

    checkpoint_vol = metis_data.CheckpointLocal(ns=dataproduct1_ns,
                                                name=checkpoint_name,
                                                path=checkpoint_loc)

    cloud_files = metis_data.CloudFiles(spark_session=session,
                                        namespace=dataproduct1_ns,
                                        stream_reader=metis_data.SparkRecursiveFileStreamer(),
                                        cloud_source=ext_vol,
                                        checkpoint_volume=checkpoint_vol,
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

    checkpoint_vol = metis_data.CheckpointLocal(ns=dataproduct1_ns,
                                                name=checkpoint_name,
                                                path=checkpoint_loc)

    cloud_files = metis_data.CloudFiles(spark_session=spark_test_session.spark_session(),
                                        namespace=dataproduct1_ns,
                                        stream_reader=metis_data.SparkRecursiveFileStreamer(),
                                        cloud_source=ext_vol,
                                        checkpoint_volume=checkpoint_vol,
                                        schema=namespaces_and_tables.json_file_schema,
                                        stream_writer=metis_data.DeltaStreamingTableWriter(),
                                        stream_to_table=sketches_table)
    df = cloud_files.read_stream()

    df2 = df.withColumns({'source_file': col("_metadata.file_path"),
                          'processing_time': current_timestamp()})

    result = cloud_files.write_stream(df2)

    sketch_df = sketches_table.read()

    assert sketch_df.count() == 4


def test_delta_table_streaming(di_initialise_spark,
                               dataproduct1_ns):
    opts = [metis_data.SparkOption.MERGE_SCHEMA]

    checkpoint_vol = metis_data.CheckpointLocal(ns=dataproduct1_ns,
                                                name=checkpoint_name,
                                                path=checkpoint_loc)

    table_cls = namespaces_and_tables.my_table_cls()

    source_table = table_cls(namespace=dataproduct1_ns,
                             checkpoint_volume=checkpoint_vol,
                             stream_reader=metis_data.DeltaStreamReader())

    stream_to_table = namespaces_and_tables.MyTable2(namespace=dataproduct1_ns,
                                                     checkpoint_volume=checkpoint_vol,
                                                     stream_writer=metis_data.DeltaStreamingTableWriter(opts))
    source_table.try_write_append(data.my_table_df())

    df = source_table.read_stream()

    assert df.isStreaming

    df2 = df.withColumns({'onStream': lit("true")})

    stream_to_table.write_stream(df2)

    df3 = stream_to_table.read()

    assert [row.onStream for row in df3.select('onStream').collect()] == ['true', 'true']
