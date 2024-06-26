import metis_data


def test_spark_naming_for_local():
    ns = metis_data.SparkCatalogueStrategy(session=None, cfg=spark_cfg())

    assert ns.namespace_name == "dp1"
    assert ns.catalogue == "domain"
    assert ns.data_product_name == "dp1"
    assert ns.fully_qualified_name("table1") == "dp1.table1"
    assert ns.data_product_root == "dp1"
    assert ns.checkpoint_volume == "/Volumes/domain/dp1/checkpoints/dp1_cp"

def test_unity_naming_for_local():
    ns = metis_data.UnityCatalogueStrategy(session=None, cfg=spark_cfg())

    assert ns.namespace_name == "dp1"
    assert ns.catalogue == "domain"
    assert ns.data_product_name == "dp1"
    assert ns.fully_qualified_name("table1") == "domain.dp1.table1"
    assert ns.data_product_root == "domain.dp1"
    assert ns.checkpoint_volume == "/Volumes/domain/dp1/checkpoints/dp1_cp"


# Helpers

def spark_cfg():
    return metis_data.Config(catalogue="domain",
                             data_product="dp1",
                             service_name="test-runner",
                             catalogue_mode=metis_data.CatalogueMode.SPARK,
                             checkpoint_name="dp1_cp")
