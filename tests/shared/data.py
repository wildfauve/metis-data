import pytest
from bevy import inject, dependency
from pyspark.sql import SparkSession, DataFrame


@inject
def my_table_df(session: SparkSession = dependency()) -> DataFrame:
    return session.read.json("tests/fixtures/table1_rows.json", multiLine=True, prefersDecimal=True)

@inject
def my_table_df_new_rows(session: SparkSession = dependency()) -> DataFrame:
    return session.read.json("tests/fixtures/table1_rows_2.json", multiLine=True, prefersDecimal=True)

@inject
def my_table_df_updated_row(session: SparkSession = dependency()) -> DataFrame:
    return session.read.json("tests/fixtures/table1_rows_3.json", multiLine=True, prefersDecimal=True)

@inject
def my_table_2_df(session: SparkSession = dependency()) -> DataFrame:
    return session.read.json("tests/fixtures/table2_rows.json", multiLine=True, prefersDecimal=True)
