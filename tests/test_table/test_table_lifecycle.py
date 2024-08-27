from tests.shared import *

import metis_data


def test_create_table_using_table_creation_protocol(di_initialise_spark,
                                                    dataproduct1_ns):
    table = Table1(namespace=dataproduct1_ns)

    assert table.table_exists()


def test_drop_table_using(di_initialise_spark,
                          dataproduct1_ns):
    table = Table1(namespace=dataproduct1_ns)

    assert table.table_exists()

    table.drop_table()

    assert not table.table_exists()


def test_noops_create_table_with_owner(di_initialise_spark,
                                       dataproduct1_ns):
    table = TableWithOwner(namespace=dataproduct1_ns,
                           table_creation_protocol=metis_data.CreateManagedDeltaTable(
                               apply_ownership_protocol=metis_data.TableOwnershipNotSupportedBuilder()))

    assert table.table_exists()


def test_performs_delta_ownership_alter():
    sess = spark_test_session.MockSession()
    sess.clear()
    metis_data.UnityTableOwnershipBuilder().apply(spark_session=sess,
                                                  fully_qualified_table_name="dp1.schema1.table1",
                                                  owner="domain-owner")

    assert sess.exprs == ['ALTER TABLE dp1.schema1.table1 SET OWNER TO domain-owner']


# Helpers

def table_definition():
    return (metis_data.Schema()
            .column()  # column1: string
            .string("column1", nullable=False)

            .column()  # column 2 struct with strings
            .struct("column2", nullable=False)
            .string("sub2.1", nullable=True)  # these have to be True as False not supported.
            .string("sub2.2", nullable=True)
            .array_struct("sub2.3", True)
            .string("sub2.3.1", True)
            .string("sub3.3.2", True)
            .end_struct()
            .end_struct())


class Table1(metis_data.DomainTable):
    table_name = "table"

    schema = table_definition().to_spark_schema()

    def after_initialise(self):
        self.perform_table_creation_protocol()


class TableWithOwner(metis_data.DomainTable):
    table_name = "table_with_owner"
    owner = "domain-owner"

    schema = table_definition().to_spark_schema()

    def after_initialise(self):
        self.perform_table_creation_protocol()
