import pytest
from bevy import inject, dependency
from delta import *
import pyspark
from pyspark.sql import SparkSession

import metis_job

from . import di


@pytest.fixture
def di_initialise_spark():
    spark = create_session()
    di.di_container().set(SparkSession, spark)


def create_session():
    return metis_job.build_spark_session("test_spark_session",
                                         spark_delta_session,
                                         spark_session_config)


def spark_delta_session(session_name):
    return configure_spark_with_delta_pip(delta_builder(session_name)).getOrCreate()


def delta_builder(session_name):
    return (pyspark.sql.SparkSession.builder.appName(session_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))


def spark_session_config(spark: pyspark.sql.session) -> None:
    pass


@inject
def spark_session(session: SparkSession = dependency()):
    return session
