import pytest

from pyspark.sql.types import DecimalType, StringType
from metis_data import schema as S
from metis_data.util import error

from tests.shared import vocab


def test_structure_dsl_with_vocab_raise_directive():
    with pytest.raises(error.VocabNotFound):
        (S.Schema(vocab=vocab.vocab(), vocab_directives=[S.VocabDirective.RAISE_WHEN_TERM_NOT_FOUND])
         .column()
         .string("not_a_column", nullable=False))


def test_schema_without_vocab():
    table = (S.Schema()
             .column()  # column1: string
             .string("column_one", nullable=False)

             .column()  # column 2 struct with strings
             .struct("column_two", nullable=False)
             .string("sub_one", nullable=False)
             .string("sub_two", nullable=False)
             .end_struct())

    expected_schema = {
        'type': 'struct',
        'fields': [
            {
                'name': 'column_one',
                'type': 'string',
                'nullable': False,
                'metadata': {}
            },
            {
                'name': 'column_two',
                'type': {
                    'type': 'struct',
                    'fields': [
                        {
                            'name': 'sub_one',
                            'type': 'string',
                            'nullable': False,
                            'metadata': {}},
                        {
                            'name': 'sub_two',
                            'type': 'string',
                            'nullable': False,
                            'metadata': {}
                        }
                    ]
                },
                'nullable': False,
                'metadata': {}
            }
        ]
    }

    assert table.to_spark_schema().jsonValue() == expected_schema


def test_column_structure_dsl():
    table = (S.Schema(vocab=vocab.vocab())
             .column()  # column1: string
             .string("columns.column1", nullable=False)

             .column()  # column 2 struct with strings
             .struct("columns.column2", nullable=False)
             .string("columns.column2.sub1", nullable=False)
             .string("columns.column2.sub1", nullable=False)
             .end_struct()

             .column()  # column 3: decimal
             .decimal("columns.column3", DecimalType(6, 3), nullable=False)

             .column()  # column 4: long
             .long("columns.column5", nullable=False)

             .column()  # column 5: array of strings
             .array("columns.column4", StringType, nullable=False)

             .column()  # column 6: array of structs
             .array_struct("columns.column6", nullable=True)
             .long("columns.column6.sub1", nullable=False)
             .string("columns.column6.sub1", nullable=False)
             .array("columns.column6.sub3", StringType, nullable=False)
             .end_struct()  # end column6

             .column()  # struct with strings and array of structs
             .struct("columns.column7", nullable=False)
             .string("columns.column7.sub1")
             .struct("columns.column7.sub2", nullable=False)
             .string("columns.column7.sub2.sub2-1")
             .string("columns.column7.sub2.sub2-2")
             .array_struct("columns.column7.sub3")
             .string("columns.column7.sub3.sub3-1")
             .string("columns.column7.sub3.sub3-2")
             .end_struct()
             .end_struct()
             .end_struct()  # end column7
             )
    assert table.to_spark_schema().jsonValue() == expected_table_schema()


def test_build_schema_at_column_level():
    c = S.Column(vocab.vocab())

    (c
     .struct("columns.column2", nullable=False)
     .string("columns.column2.sub1", nullable=False)
     .string("columns.column2.sub1", nullable=False)
     .end_struct())

    expected_schema = {'type': 'struct', 'fields': [{'name': 'column_two', 'type': {'type': 'struct', 'fields': [
        {'name': 'sub_one', 'type': 'string', 'nullable': False, 'metadata': {}},
        {'name': 'sub_one', 'type': 'string', 'nullable': False, 'metadata': {}}]}, 'nullable': False, 'metadata': {}}]}

    assert c.to_spark_schema(as_root=True).jsonValue() == expected_schema


def test_build_column_and_add_to_table():
    table = S.Schema(vocab=vocab.vocab())

    c = S.Column(vocab.vocab())

    (c.struct("columns.column2", nullable=False)
     .string("columns.column2.sub1", nullable=False)
     .string("columns.column2.sub1", nullable=False)
     .end_struct())

    table.add_column(c)

    expected_schema = {'type': 'struct', 'fields': [{'name': 'column_two', 'type': {'type': 'struct', 'fields': [
        {'name': 'sub_one', 'type': 'string', 'nullable': False, 'metadata': {}},
        {'name': 'sub_one', 'type': 'string', 'nullable': False, 'metadata': {}}]}, 'nullable': False, 'metadata': {}}]}

    assert table.to_spark_schema().jsonValue() == expected_schema
    assert table.to_spark_schema().jsonValue() == expected_schema


def test_mix_dsl_and_independent_columns():
    col2 = S.Column(vocab.vocab())

    (col2.struct("columns.column2", nullable=False)
     .string("columns.column2.sub1", nullable=False)
     .string("columns.column2.sub1", nullable=False)
     .end_struct())

    table = (S.Schema(vocab=vocab.vocab())

             .column()  # column1: string
             .string("columns.column1", nullable=False)

             .add_column(col2)

             .column()  # column 3: decimal
             .decimal("columns.column3", DecimalType(6, 3), nullable=False))

    expected_schema = {'type': 'struct',
                       'fields': [{'name': 'column_one', 'type': 'string', 'nullable': False, 'metadata': {}},
                                  {'name': 'column_two', 'type': {'type': 'struct', 'fields': [
                                      {'name': 'sub_one', 'type': 'string', 'nullable': False, 'metadata': {}},
                                      {'name': 'sub_one', 'type': 'string', 'nullable': False, 'metadata': {}}]},
                                   'nullable': False, 'metadata': {}},
                                  {'name': 'column_three', 'type': 'decimal(6,3)', 'nullable': False, 'metadata': {}}]}

    assert table.to_spark_schema().jsonValue() == expected_schema


# Helpers

def expected_table_schema():
    return {'type': 'struct', 'fields': [{'name': 'column_one', 'type': 'string', 'nullable': False, 'metadata': {}},
                                         {'name': 'column_two', 'type': {'type': 'struct', 'fields': [
                                             {'name': 'sub_one', 'type': 'string', 'nullable': False, 'metadata': {}},
                                             {'name': 'sub_one', 'type': 'string', 'nullable': False, 'metadata': {}}]},
                                          'nullable': False, 'metadata': {}},
                                         {'name': 'column_three', 'type': 'decimal(6,3)', 'nullable': False,
                                          'metadata': {}},
                                         {'name': 'column_five', 'type': 'long', 'nullable': False, 'metadata': {}},
                                         {'name': 'column_four',
                                          'type': {'type': 'array', 'elementType': 'string', 'containsNull': True},
                                          'nullable': False, 'metadata': {}}, {'name': 'column_six',
                                                                               'type': {'type': 'array',
                                                                                        'elementType': {
                                                                                            'type': 'struct',
                                                                                            'fields': [
                                                                                                {'name': 'sub_one',
                                                                                                 'type': 'long',
                                                                                                 'nullable': False,
                                                                                                 'metadata': {}},
                                                                                                {'name': 'sub_one',
                                                                                                 'type': 'string',
                                                                                                 'nullable': False,
                                                                                                 'metadata': {}},
                                                                                                {'name': 'sub_three',
                                                                                                 'type': {
                                                                                                     'type': 'array',
                                                                                                     'elementType': 'string',
                                                                                                     'containsNull': True},
                                                                                                 'nullable': False,
                                                                                                 'metadata': {}}]},
                                                                                        'containsNull': True},
                                                                               'nullable': True, 'metadata': {}},
                                         {'name': 'column_seven', 'type': {'type': 'struct', 'fields': [
                                             {'name': 'sub_one', 'type': 'string', 'nullable': False, 'metadata': {}},
                                             {'name': 'sub_two', 'type': {'type': 'struct', 'fields': [
                                                 {'name': 'sub_two_one', 'type': 'string', 'nullable': False,
                                                  'metadata': {}},
                                                 {'name': 'sub_two_two', 'type': 'string', 'nullable': False,
                                                  'metadata': {}}, {'name': 'sub_three', 'type': {'type': 'array',
                                                                                                  'elementType': {
                                                                                                      'type': 'struct',
                                                                                                      'fields': [{
                                                                                                                     'name': 'sub_three_one',
                                                                                                                     'type': 'string',
                                                                                                                     'nullable': False,
                                                                                                                     'metadata': {}},
                                                                                                                 {
                                                                                                                     'name': 'sub_three_two',
                                                                                                                     'type': 'string',
                                                                                                                     'nullable': False,
                                                                                                                     'metadata': {}}]},
                                                                                                  'containsNull': True},
                                                                    'nullable': False, 'metadata': {}}]},
                                              'nullable': False, 'metadata': {}}]}, 'nullable': False, 'metadata': {}}]}
