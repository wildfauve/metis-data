from functools import partial
from typing import Tuple
from pymonad.maybe import Just, Nothing, Maybe


def create_db(db_name: str,
              db_property_expression: str = None) -> Maybe[str]:
    """
    This is equivalent to CREATE SCHEMA.
    """
    return (Just([])
            .maybe(None, partial(create_db_base, db_name))
            .maybe(None, partial(db_props, db_property_expression))
            .maybe(None, maybe_joiner))


def describe_schema(db_name: str) -> Maybe[str]:
    """
    This is equivalent to CREATE DATABASE.
    """
    return (Just([])
            .maybe(None, partial(describe_db_base, db_name))
            .maybe(None, maybe_joiner))


def set_owner_of_schema(db_name: str, owner: str) -> Maybe[str]:
    """
    This is equivalent to CREATE DATABASE.
    """
    return (Just([])
            .maybe(None, partial(alter_schema_base, db_name))
            .maybe(None, partial(set_owner, owner))
            .maybe(None, maybe_joiner))


def set_owner_of_table(table_name: str, owner: str) -> Maybe[str]:
    """
    This is equivalent to CREATE DATABASE.
    """
    return (Just([])
            .maybe(None, partial(alter_table_base, table_name))
            .maybe(None, partial(set_owner, owner))
            .maybe(None, maybe_joiner))


def set_owner_of_volume(volume: str, owner: str):
    return (Just([])
            .maybe(None, partial(alter_volume_base, volume))
            .maybe(None, partial(set_owner, owner))
            .maybe(None, maybe_joiner))


def create_managed_volume(volume_name: str) -> Maybe[str]:
    """
    """
    return (Just([])
            .maybe(None, partial(_create_managed_volume, volume_name))
            .maybe(None, maybe_joiner))


def create_external_volume(volume_name: str,
                           volume_location: str) -> Maybe[str]:
    """
    This is equivalent to CREATE SCHEMA.
    """
    return (Just([])
            .maybe(None, partial(create_ext_vol_base, volume_name))
            .maybe(None, partial(with_volume_location, volume_location))
            .maybe(None, maybe_joiner))


def create_ext_vol_base(name, expr) -> Maybe[list]:
    return Just(expr + [f"CREATE EXTERNAL VOLUME IF NOT EXISTS {name}"])


def _create_managed_volume(name, expr) -> Maybe[list]:
    return Just(expr + [f"CREATE VOLUME IF NOT EXISTS {name}"])


def with_volume_location(name, expr) -> Maybe[list]:
    return Just(expr + [f"LOCATION '{name}'"])


def describe_db_base(db_name: str, expr) -> Maybe[list]:
    return Just(expr + [f"DESCRIBE SCHEMA {db_name}"])


def alter_schema_base(db_name: str, expr) -> Maybe[list]:
    return Just(expr + [f"ALTER SCHEMA {db_name}"])


def alter_table_base(table_name: str, expr) -> Maybe[list]:
    return Just(expr + [f"ALTER TABLE {table_name}"])


def alter_volume_base(volume: str, expr) -> Maybe[list]:
    return Just(expr + [f"ALTER VOLUME {volume}"])


def set_owner(owner_name: str, expr) -> Maybe[list]:
    return Just(expr + [f"SET OWNER TO {owner_name}"])


def describe_db(db_name: str) -> str:
    sql = [f"DESCRIBE DATABASE EXTENDED {db_name}"]
    return joiner(sql)


def create_db_base(db_name: str, expr: list) -> Maybe[list]:
    return Just(expr + [f"create database IF NOT EXISTS {db_name}"])


def db_props(props: str, expr: list) -> Maybe[list]:
    if not props:
        return Just(expr)
    return Just(expr + [f" WITH DBPROPERTIES ( {props} )"])


def create_unmanaged_table(table_name: str,
                           col_specification: str,
                           location: str,
                           partition_clause: Tuple = None,
                           table_property_expression: str = None):
    sql = base_create_table(table_name=table_name,
                            col_specification=col_specification,
                            partition_clause=partition_clause,
                            table_property_expression=table_property_expression)
    sql.append(f"LOCATION '{location}'")
    return joiner(sql)


def create_managed_table(table_name: str,
                         col_specification: str,
                         partition_clause: Tuple = None,
                         table_property_expression: str = None):
    return joiner(base_create_table(table_name=table_name,
                                    col_specification=col_specification,
                                    partition_clause=partition_clause,
                                    table_property_expression=table_property_expression))


def base_create_table(table_name: str,
                      col_specification: str,
                      partition_clause: Tuple = None,
                      table_property_expression: str = None) -> list[str]:
    tbl_props = f"TBLPROPERTIES ( {table_property_expression} )" if table_property_expression else None
    partition_on = f"PARTITIONED BY ( {','.join(partition_clause)} )" if partition_clause else None

    sql = [f"CREATE TABLE IF NOT EXISTS {table_name} ( {col_specification} ) USING DELTA"]
    sql.append(partition_on) if partition_on else sql
    sql.append(tbl_props) if tbl_props else sql
    return sql


def drop_table(table_name):
    return (Just([])
            .maybe(None, partial(_drop_table, table_name))
            .maybe(None, maybe_joiner))


def _drop_table(name, expr) -> Maybe[list]:
    return Just(expr + [f"DROP TABLE IF EXISTS {name}"])


def show_properties(table_name):
    return joiner([f"SHOW TBLPROPERTIES {table_name}"])


def set_properties(table_name: str, props: str):
    return joiner([f"alter table {table_name} SET TBLPROPERTIES ({props})"])


def unset_properties(table_name: str, props: str):
    return joiner([f"alter table {table_name} UNSET TBLPROPERTIES ({props})"])


def joiner(expr: list):
    return " ".join(expr)


def maybe_joiner(expr: list) -> Maybe[list]:
    return Just(" ".join(expr))


def _to_maybe(val):
    return Just(val) if val else Nothing
