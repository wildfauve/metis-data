import metis_data


class MockSession:
    exprs = []

    def clear(self):
        self.exprs = []

    def sql(self, expr):
        self.exprs.append(expr)


def test_spark_naming_for_local():
    ns = metis_data.SparkCatalogueStrategy(session=None, cfg=spark_cfg())

    assert ns.namespace_name == "dp1"
    assert ns.catalogue == "domain"
    assert ns.data_product_name == "dp1"
    assert ns.fully_qualified_name("table1") == "dp1.table1"
    assert ns.data_product_root == "dp1"
    assert ns.checkpoint_volume() == "/Volumes/domain/dp1/checkpoints/dp1_cp"


def test_unity_naming_for_local():
    ns = metis_data.UnityCatalogueStrategy(session=None, cfg=spark_cfg())

    assert ns.namespace_name == "dp1"
    assert ns.catalogue == "domain"
    assert ns.data_product_name == "dp1"
    assert ns.fully_qualified_name("table1") == "domain.dp1.table1"
    assert ns.data_product_root == "domain.dp1"
    assert ns.checkpoint_volume() == "/Volumes/domain/dp1/checkpoints/dp1_cp"


def test_unity_create_schema():
    sess = MockSession()
    sess.clear()
    ns = metis_data.UnityCatalogueStrategy(session=sess, cfg=spark_cfg())

    ns.create("")
    assert sess.exprs[0] == 'create database IF NOT EXISTS domain.dp1'


def test_unity_create_ext_vol():
    sess = MockSession()
    sess.clear()
    ns = metis_data.NameSpace(session=sess, cfg=unity_cfg())

    ext_vol = metis_data.S3ExternalVolumeSource(ns=ns,
                                                name="events",
                                                source="/Volume/domain/data_product",
                                                external_bucket="s3://bucket/folder")

    expected_exprs = ['create database IF NOT EXISTS domain.dp1',
                      "CREATE EXTERNAL VOLUME IF NOT EXISTS domain.dp1.events LOCATION 's3://bucket/folder'"]

    assert sess.exprs == expected_exprs


def test_unity_create_checkpoint_vol():
    sess = MockSession()
    sess.clear()
    ns = metis_data.NameSpace(session=sess, cfg=unity_cfg())

    ext_vol = metis_data.DeltaCheckpoint(ns=ns,
                                         name="checkpoints")

    expected_expr = 'CREATE VOLUME IF NOT EXISTS domain.dp1.checkpoints'

    assert expected_expr in sess.exprs


# Helpers

def spark_cfg():
    return metis_data.Config(catalogue="domain",
                             data_product="dp1",
                             service_name="test-runner",
                             catalogue_mode=metis_data.CatalogueMode.SPARK,
                             checkpoint_name="dp1_cp")


def unity_cfg():
    return metis_data.Config(catalogue="domain",
                             data_product="dp1",
                             service_name="test-runner",
                             catalogue_mode=metis_data.CatalogueMode.UNITY,
                             checkpoint_name="dp1_cp")
