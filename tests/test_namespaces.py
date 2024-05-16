import pytest

def it_creates_the_namespace(di_initialise_spark,
                             dataproduct1_ns):
    assert dataproduct1_ns.namespace_exists()


# Helpers


