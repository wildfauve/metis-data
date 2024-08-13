from metis_data.repo import sql_builder

def test_builds_create_schema():
    result = sql_builder.create_db("cat1.schema1")

    assert result.value == "create database IF NOT EXISTS cat1.schema1"
