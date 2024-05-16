from metis_job import repo


def test_merge_options():
    options = [repo.SparkOption.MERGE_SCHEMA]

    assert repo.SparkOption.function_based_options(options) == {'mergeSchema': 'true'}


def test_options_to_spark_options():
    options = [repo.SparkOption.MERGE_SCHEMA]

    expected = [('spark.databricks.delta.schema.autoMerge.enabled', 'true')]

    assert repo.SparkOption.options_to_spark_options(options) == expected


def test_options_to_spark_option_names():
    options = [repo.SparkOption.MERGE_SCHEMA]

    expected = ['spark.databricks.delta.schema.autoMerge.enabled']

    assert repo.SparkOption.options_to_spark_option_names(options) == expected
