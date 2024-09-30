import logging

import metis_data
from tests.shared import namespaces_and_tables, spark_test_session


def test_spark_naming_for_local():
    ns = metis_data.SparkCatalogueStrategy(session=None, cfg=spark_cfg())

    assert ns.namespace_name == "dp1"
    assert ns.catalogue == "domain"
    assert ns.data_product_name == "dp1"
    assert ns.fully_qualified_name("table1") == "dp1.table1"
    assert ns.data_product_root == "dp1"
    assert ns.checkpoint_volume(ns.cfg.checkpoint_volume, "dp1_cp") == "/Volumes/domain/dp1/checkpoints/dp1_cp"


def test_unity_naming_for_local():
    ns = metis_data.UnityCatalogueStrategy(session=None, cfg=spark_cfg())

    assert ns.namespace_name == "dp1"
    assert ns.catalogue == "domain"
    assert ns.data_product_name == "dp1"
    assert ns.fully_qualified_name("table1") == "domain.dp1.table1"
    assert ns.data_product_root == "domain.dp1"
    assert ns.checkpoint_volume(ns.cfg.checkpoint_volume, "dp1_cp") == "/Volumes/domain/dp1/checkpoints/dp1_cp"


def test_unity_create_schema():
    sess = spark_test_session.MockSession()
    sess.clear()
    ns = metis_data.UnityCatalogueStrategy(session=sess, cfg=spark_cfg())

    ns.create("")
    assert sess.exprs[0] == 'create database IF NOT EXISTS domain.dp1'


def test_unity_create_schema_with_owner():
    sess = spark_test_session.MockSession()
    sess.clear()
    ns = metis_data.UnityCatalogueStrategy(session=sess, cfg=unity_cfg_with_schema_owner())

    ns.create("")
    expected = ['create database IF NOT EXISTS domain.dp1',
                'ALTER SCHEMA domain.dp1 SET OWNER TO domain-owner']
    assert sess.exprs == expected


def test_unity_create_ext_vol():
    sess = spark_test_session.MockSession()
    sess.clear()
    ns = metis_data.NameSpace(session=sess, cfg=unity_cfg())

    ext_vol = metis_data.S3ExternalVolumeSource(ns=ns,
                                                name="events",
                                                source="/Volume/domain/data_product",
                                                external_bucket="s3://bucket/folder")

    expected_expr = "CREATE EXTERNAL VOLUME IF NOT EXISTS domain.dp1.events LOCATION 's3://bucket/folder'"
    assert expected_expr in sess.exprs


def test_unity_create_ext_vol_with_owner():
    sess = spark_test_session.MockSession()
    sess.clear()
    ns = metis_data.NameSpace(session=sess, cfg=unity_cfg_with_schema_owner())

    ext_vol = metis_data.S3ExternalVolumeSource(ns=ns,
                                                name="events",
                                                source="/Volume/domain/data_product",
                                                external_bucket="s3://bucket/folder")

    expected_expr = "ALTER VOLUME domain.dp1.events SET OWNER TO domain-owner"
    assert expected_expr in sess.exprs


def test_unity_create_checkpoint_vol():
    sess = spark_test_session.MockSession()
    sess.clear()

    metis_data.NameSpace(session=sess, cfg=unity_cfg())

    expected_expr = 'CREATE VOLUME IF NOT EXISTS domain.dp1.checkpoints'

    assert expected_expr in sess.exprs

    assert all(["OWNER" not in expr for expr in sess.exprs])


def test_unity_create_checkpoint_vol_set_owner():
    sess = spark_test_session.MockSession()
    sess.clear()

    metis_data.NameSpace(session=sess, cfg=unity_cfg_with_schema_owner())

    expected_expr = 'ALTER VOLUME domain.dp1.checkpoints SET OWNER TO domain-owner'

    assert expected_expr in sess.exprs


# Helpers

def spark_cfg():
    return metis_data.Config(catalogue="domain",
                             data_product="dp1",
                             service_name="test-runner",
                             catalogue_mode=metis_data.CatalogueMode.SPARK,
                             checkpoint_volume=metis_data.CheckpointVolumeWithPath(name="checkpoints",
                                                                                   path=namespaces_and_tables.CHECKPOINT_PATH))


def unity_cfg():
    return metis_data.Config(catalogue="domain",
                             data_product="dp1",
                             service_name="test-runner",
                             catalogue_mode=metis_data.CatalogueMode.UNITY,
                             checkpoint_volume=metis_data.CheckpointVolumeRoot(name="checkpoints"),
                             log_level=logging.DEBUG)


def unity_cfg_with_schema_owner():
    return metis_data.Config(catalogue="domain",
                             data_product="dp1",
                             service_name="test-runner",
                             owner="domain-owner",
                             catalogue_mode=metis_data.CatalogueMode.UNITY,
                             checkpoint_volume=metis_data.CheckpointVolumeRoot(name="checkpoints"),
                             log_level=logging.DEBUG)
