from tests.shared import *

import metis_data as J


def test_create_file_from_dsl_structure(di_initialise_spark,
                                        dataproduct1_ns):
    table1 = Table1(namespace=dataproduct1_ns)

    df = table1.read()

    assert df.schema == table1_definition().to_spark_schema()


# Helpers

def table1_definition():
    return (J.Schema(vocab=vocab())
            .column()  # column1: string
            .string("columns.column1", nullable=False)

            .column()  # column 2 struct with strings
            .struct("columns.column2", nullable=False)
            .string("columns.column2.sub1", nullable=True)  # these have to be True as False not supported.
            .string("columns.column2.sub2", nullable=True)
            .array_struct("columns.column2.sub3", True)
            .string("columns.column2.sub3.sub3-1", True)
            .string("columns.column2.sub3.sub3-2", True)
            .end_struct()
            .end_struct())


def vocab():
    return {
        "columns": {
            "column1": {
                "term": "column_one"
            },
            "column2": {
                "term": "column_two",
                "sub1": {
                    "term": "sub_one"
                },
                "sub2": {
                    "term": "sub_two"
                },
                "sub3": {
                    "term": "sub_three",
                    "sub3-1": {
                        "term": "sub_three_one"
                    },
                    "sub3-2": {
                        "term": "sub_three_two"
                    }
                }
            }
        }
    }


class Table1(J.DomainTable):
    table_name = "table1"

    schema = table1_definition().to_spark_schema()

    def after_initialise(self):
        self.perform_table_creation_protocol()

