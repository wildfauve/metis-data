from __future__ import annotations
import metis_job
from . import spark_util


class SparkRecursiveFileStreamer:
    default_spark_options = [spark_util.SparkOption.RECURSIVE_LOOKUP]

    def __init__(self, spark_options: list[spark_util.SparkOption] = None):
        self.spark_options = spark_options if spark_options else []

    def read_stream(self,
                    cloud_file: metis_job.CloudFiles,):
        return (cloud_file.spark_session
                .readStream
                .options(**self._spark_opts())
                .schema(cloud_file.schema)
                .json(cloud_file.cloud_source, multiLine=True, prefersDecimal=True))

    def _spark_opts(self):
        return metis_job.SparkOption.function_based_options(self.__class__.default_spark_options + self.spark_options)


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
    > opts = [metis_job.SparkOptions.JSON_CLOUD_FILES_FORMAT]  # only JSON is supported.
    > DatabricksCloudFilesStreamer(spark_options=opts)
    """
    format = "cloudFiles"
    default_spark_options = []

    def __init__(self, spark_options: list[spark_util.SparkOption] = None):
        self.spark_options = spark_options if spark_options else []


    def read_stream(self,
                    cloud_file: metis_job.CloudFiles):
        return (cloud_file.spark_session
                .readStream
                .format(self.__class__.format)
                .options(**self._spark_opts(cloud_file))
                .schema(cloud_file.schema)
                .load(cloud_file.cloud_source))

    def _spark_opts(self, cloud_file):
        opts = spark_util.SparkOption.function_based_options(self.__class__.default_spark_options + self.spark_options)
        return {**opts,
                **{'cloudFiles.schemaLocation': cloud_file.checkpoint_location}}
