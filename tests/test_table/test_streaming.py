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


# def test_for_spark_based_api_for_streaming_files(di_initialise_spark):
#     stream_source = "tests/spark_locations/stream_source"
#     checkpoint_loc = 'tests/spark_locations/checkpoints'
#
#     table = "sketch"
#     opts = [metis_data.SparkOption.RECURSIVE_LOOKUP]
#
#     df = (spark_test_session.spark_session()
#           .readStream.options(**metis_data.SparkOption.function_based_options(opts))
#           .schema(schema())
#           .json(stream_source, multiLine=True))
#
#     df2 = df.select("*",
#                     col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
#
#     df2.writeStream.option('checkpointLocation', checkpoint_loc).trigger(availableNow=True).toTable(table)



def test_cloud_files_streaming(di_initialise_spark,
                               dataproduct1_ns):
    stream_source = "tests/spark_locations/stream_source"
    checkpoint_loc = 'tests/spark_locations/checkpoints'

    opts = [metis_data.SparkOption.MERGE_SCHEMA]

    cloud_files = metis_data.CloudFiles(spark_session=spark_test_session.spark_session(),
                                       stream_reader=metis_data.SparkRecursiveFileStreamer(),
                                       cloud_source=stream_source,
                                       checkpoint_location=checkpoint_loc,
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
                                       stream_reader=metis_data.SparkRecursiveFileStreamer(),
                                       cloud_source=stream_source,
                                       checkpoint_location=checkpoint_loc,
                                       schema=namespaces_and_tables.json_file_schema,
                                       stream_writer=metis_data.DeltaStreamingTableWriter(),
                                       stream_to_table=sketches_table)
    df = cloud_files.read_stream()

    df2 = df.withColumns({'source_file': col("_metadata.file_path"),
                          'processing_time': current_timestamp()})

    result = cloud_files.write_stream(df2)

    sketch_df = sketches_table.read()

    assert sketch_df.count() == 4
