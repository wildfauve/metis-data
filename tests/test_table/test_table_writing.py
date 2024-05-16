from tests.shared import *

import metis_job as J

from tests.shared import namespaces_and_tables, data


def test_delta_upsert_no_change(di_initialise_spark,
                                dataproduct1_ns):
    table_cls = namespaces_and_tables.my_table_cls()

    my_table = table_cls(namespace=dataproduct1_ns)

    df = data.my_table_df()

    result = my_table.try_write_append(df)

    assert result.is_right()

    df = my_table.read()

    assert df.count() == 2

    my_table.try_upsert(df)

    df = my_table.read()

    assert df.count() == 2

    sketches = [(row.id, row.name) for row in df.select(df.id, df.name).collect()]

    expected_results = [('https://example.nz/montyPython/sketches/theSpanishInquisition', 'The Spanish Inquisition'),
                        ('https://example.nz/montyPython/sketches/thePiranhaBrothers', 'The Piranha Brothers')]

    assert sketches == expected_results


def test_partitioned_delta_upsert_with_new_rows(di_initialise_spark,
                                                dataproduct1_ns):
    table_cls = namespaces_and_tables.my_table_cls()

    my_table = table_cls(namespace=dataproduct1_ns)

    my_table.try_write_append(data.my_table_df())

    df = my_table.read()

    assert df.count() == 2

    my_table.try_upsert(data.my_table_df_new_rows())

    df = my_table.read()

    assert df.count() == 4

    sketches = set([(row.id, row.name) for row in df.select(df.id, df.name).collect()])

    expected_rows = set([('https://example.nz/montyPython/sketches/thePiranhaBrothers', 'The Piranha Brothers'),
                         ('https://example.nz/montyPython/sketches/theSpanishInquisition', 'The Spanish Inquisition'),
                         ('https://example.nz/montyPython/sketches/ericTheHalfBee', 'Eric the Half Bee'),
                         ('https://example.nz/montyPython/sketches/theCheeseShop', 'The Cheese Shop')])

    assert sketches == expected_rows


def test_upsert_updates_row(di_initialise_spark,
                            dataproduct1_ns):
    table_cls = namespaces_and_tables.my_table_cls()

    my_table = table_cls(namespace=dataproduct1_ns)

    my_table.try_write_append(data.my_table_df())

    df = my_table.read()

    assert df.count() == 2

    my_table.try_upsert(data.my_table_df_updated_row())

    df = my_table.read()

    assert df.count() == 2

    new_pythons = [python.id for row in
                   df.filter(df.id == 'https://example.nz/montyPython/sketches/thePiranhaBrothers').select(
                       df.pythons).collect() for
                   python in row.pythons]

    expected_updates = ['https://example.nz/montyPython/michealPalin',
                        'https://example.nz/montyPython/johnCleese',
                        'https://example.nz/montyPython/ericIdol']

    assert new_pythons == expected_updates

# Helpers
