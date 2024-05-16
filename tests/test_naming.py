import metis_job


def test_spark_naming_for_local():
    naming = metis_job.SparkNamingConventionDomainBased(job_config=spark_naming())

    assert naming.namespace_name() == "dp1"
    assert naming.catalogue() == "domain"
    assert naming.data_product_name() == "dp1"
    assert naming.fully_qualified_name("table1") == "dp1.table1"
    assert naming.data_product_root() == "dp1"

def test_unity_naming_for_local():
    naming = metis_job.UnityNamingConventionDomainBased(job_config=spark_naming())

    assert naming.namespace_name() == "dp1"
    assert naming.catalogue() == "domain"
    assert naming.data_product_name() == "dp1"
    assert naming.fully_qualified_name("table1") == "domain.dp1.table1"
    assert naming.data_product_root() == "domain.dp1"


# Helpers

def spark_naming():
    return metis_job.Config(catalogue="domain",
                            data_product="dp1",
                            service_name="test-runner",
                            job_mode=metis_job.JobMode.SPARK)
