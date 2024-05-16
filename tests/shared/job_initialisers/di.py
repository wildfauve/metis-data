from bevy import get_repository
from pyspark.sql import SparkSession

from metis_job.util import monad

import metis_job
from tests.shared import spark_test_session, namespaces_and_tables

@metis_job.initialiser_register(order=1)
def di_container():
    di = get_repository()

    spark = spark_test_session.create_session()
    di.set(SparkSession, spark)

    cfg, ns = namespaces_and_tables.dp1_cfg_ns()
    di.set(metis_job.Config, cfg)

    di.set(metis_job.NameSpace, ns)

    table_cls = namespaces_and_tables.my_table2_cls()
    table = table_cls(ns)

    di.set(table_cls, table)
    return monad.Right(di)
