import metis_data


def test_spark_naming_for_local():
    naming = metis_data.SparkCatalogueStrategy(session=None, cfg=spark_cfg())

    assert naming.namespace_name() == "dp1"
    assert naming.catalogue() == "domain"
    assert naming.data_product_name() == "dp1"
    assert naming.fully_qualified_name("table1") == "dp1.table1"
    assert naming.data_product_root() == "dp1"

def test_unity_naming_for_local():
    naming = metis_data.UnityCatalogueStrategy(session=None, cfg=spark_cfg())

    assert naming.namespace_name() == "dp1"
    assert naming.catalogue() == "domain"
    assert naming.data_product_name() == "dp1"
    assert naming.fully_qualified_name("table1") == "domain.dp1.table1"
    assert naming.data_product_root() == "domain.dp1"


# Helpers

def spark_cfg():
    return metis_data.Config(catalogue="domain",
                             data_product="dp1",
                             service_name="test-runner",
                             catalogue_mode=metis_data.CatalogueMode.SPARK)
