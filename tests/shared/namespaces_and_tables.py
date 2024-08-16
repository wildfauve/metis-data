import pytest
import metis_data
from pyspark.sql import types as T

from metis_data import namespace

from . import *
from . import di

CHECKPOINT_PATH = "tests/spark_locations"

table_schema = T.StructType(
    [
        T.StructField('id', T.StringType(), True),
        T.StructField('isDeleted', T.StringType(), True),
        T.StructField('name', T.StringType(), True),
        T.StructField('pythons',
                      T.ArrayType(T.StructType([T.StructField('id', T.StringType(), True)]), True),
                      True),
        T.StructField('season', T.StringType(), True)
    ])

json_file_schema = table_schema

streaming_to_table_schema = T.StructType(
    [
        T.StructField('id', T.StringType(), True),
        T.StructField('isDeleted', T.StringType(), True),
        T.StructField('name', T.StringType(), True),
        T.StructField('pythons',
                      T.ArrayType(T.StructType([T.StructField('id', T.StringType(), True)]), True),
                      True),
        T.StructField('season', T.StringType(), True),
        T.StructField('source_file', T.StringType(), True),
        T.StructField('processing_time', T.TimestampType(), True)
    ])


@pytest.fixture
def namespace_wrapper():
    yield
    di.di_container().get(metis_data.NameSpace).drop()


@pytest.fixture
def dataproduct1_ns():
    cfg, ns = dp1_cfg_ns()

    yield ns

    ns.drop()


def dp1_cfg_ns():
    config = metis_data.Config(catalogue="testDomain",
                               data_product="dp1",
                               service_name="test-runner",
                               catalogue_mode=metis_data.CatalogueMode.SPARK,
                               checkpoint_volume=metis_data.CheckpointVolumeWithPath(name="checkpoints",
                                                                                     path=CHECKPOINT_PATH),
                               namespace_strategy_cls=SparkCatalogueStrategyForTesting)

    namespace = metis_data.NameSpace(session=spark_test_session.spark_session(),
                                     cfg=config)
    return config, namespace


def my_table_cls():
    class MyTable(metis_data.DomainTable):
        table_name = "my_table"
        temp_table_name = "_temp_my_hive_table"
        partition_columns = ("name",)
        pruning_column = 'name'

        table_properties = [
            metis_data.TableProperty(metis_data.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace")
        ]

        schema = table_schema

        def after_append(self, result):
            self.properties.merge_table_properties()

        def identity_merge_condition(self, name_of_baseline, update_name):
            return f"{name_of_baseline}.id = {update_name}.id"

    return MyTable


class SparkCatalogueStrategyForTesting(namespace.SparkCatalogueStrategy):

    def __init__(self, session, cfg):
        super().__init__(session, cfg)

    def checkpoint_volume(self, root: metis_data.CheckpointVolumeRoot, name: str) -> str:
        return f"{root.path}/{root.name}/{name}"


class MyTable2(metis_data.DomainTable):
    table_name = "my_table_2"

    table_creation_protocol = metis_data.CreateManagedDeltaTable

    partition_columns = ("name",)

    pruning_column = 'name'

    schema = table_schema

    def after_initialise(self):
        self.perform_table_creation_protocol()

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"


class MyStreamToTable(metis_data.DomainTable):
    table_name = "my_stream_to_table"

    table_creation_protocol = metis_data.CreateManagedDeltaTable

    partition_columns = ("name",)

    pruning_column = 'name'

    schema = streaming_to_table_schema

    def after_initialise(self):
        self.perform_table_creation_protocol()

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"


def my_table2_cls(streaming_table: bool = False):
    if streaming_table:
        return MyStreamToTable
    return MyTable2


def my_table_with_props_cls():
    class MyTableCreatedWithProps(metis_data.DomainTable):
        table_creation_protocol = metis_data.CreateManagedDeltaTable

        table_name = "my_hive_table_created_as_managed_table"

        table_properties = [
            metis_data.TableProperty(metis_data.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace"),
            metis_data.TableProperty(metis_data.DataAgreementType.PARTITION_COLUMNS, "identity", "my_namespace"),
            metis_data.TableProperty(metis_data.DataAgreementType.PRUNE_COLUMN, "identity", "my_namespace"),
            metis_data.TableProperty(metis_data.DataAgreementType.PORT, "superTable", "my_namespace"),
            metis_data.TableProperty(metis_data.DataAgreementType.UPDATE_FREQUENCY, "daily", "my_namespace"),
            metis_data.TableProperty(metis_data.DataAgreementType.DESCRIPTION, "Some description", "my_namespace"),
        ]

        schema = table_schema

        def after_initialise(self):
            self.perform_table_creation_protocol()
            pass

        def identity_merge_condition(self, name_of_baseline, update_name):
            return f"{name_of_baseline}.id = {update_name}.id"

    return MyTableCreatedWithProps
