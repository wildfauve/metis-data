import shutil
from pathlib import Path
from pyspark.sql.functions import col, current_timestamp

import metis_data

from tests.shared import spark_test_session, namespaces_and_tables

checkpoint_path = Path("tests") / "spark_locations" / "checkpoints"


def setup_function():
    if checkpoint_path.exists():
        shutil.rmtree(checkpoint_path)
    checkpoint_path.mkdir(parents=True, exist_ok=True)


def test_cloud_files_streaming(di_initialise_spark,
                               dataproduct1_ns):
    stream_source = "tests/spark_locations/stream_source"
    checkpoint_loc = 'tests/spark_locations/checkpoints'

    opts = [metis_data.SparkOption.MERGE_SCHEMA]

    cloud_files = metis_data.CloudFiles(spark_session=spark_test_session.spark_session(),
                                        namespace=dataproduct1_ns,
                                        stream_reader=metis_data.SparkRecursiveFileStreamer(),
                                        cloud_source=stream_source,
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
    stream_source = "tests/spark_locations/stream_source"
    checkpoint_loc = 'tests/spark_locations/checkpoints'

    sketches_table = namespaces_and_tables.my_table2_cls(streaming_table=True)(namespace=dataproduct1_ns)

    cloud_files = metis_data.CloudFiles(spark_session=spark_test_session.spark_session(),
                                        namespace=dataproduct1_ns,
                                        stream_reader=metis_data.SparkRecursiveFileStreamer(),
                                        cloud_source=stream_source,
                                        schema=namespaces_and_tables.json_file_schema,
                                        stream_writer=metis_data.DeltaStreamingTableWriter(),
                                        stream_to_table=sketches_table)
    df = cloud_files.read_stream()

    df2 = df.withColumns({'source_file': col("_metadata.file_path"),
                          'processing_time': current_timestamp()})

    result = cloud_files.write_stream(df2)

    sketch_df = sketches_table.read()

    assert sketch_df.count() == 4
