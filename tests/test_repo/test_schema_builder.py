from metis_data.repo import sql_builder

def test_builds_create_schema():
    result = sql_builder.create_db("cat1.schema1")

    assert result.value == "create database IF NOT EXISTS cat1.schema1"


def test_builds_external_vol_escaping_hyphen():
    result = sql_builder.create_external_volume("cat1.schema1.vol-1", "s3://bucket/folder")

    assert result.value ==  "CREATE EXTERNAL VOLUME IF NOT EXISTS `cat1.schema1.vol-1` LOCATION 's3://bucket/folder'"