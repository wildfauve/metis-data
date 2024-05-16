from bevy import get_repository
from pyspark.sql.session import SparkSession

from metis_data.util import error, monad
from . import helpers

from tests.shared import spark_test_session

@helpers.register(order=1)
def di_initialisation():
    return initialise_di()


@monad.Try(error_cls=error.DIInitialisationError)
def initialise_di():
    di = get_repository()
    [f(di) for f in [spark_session]]


def spark_session(di):
    spark = spark_test_session.create_session()
    di.set(SparkSession, spark)
