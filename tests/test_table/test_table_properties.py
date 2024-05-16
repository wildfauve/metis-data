import metis_data

from tests.shared import data, namespaces_and_tables


def test_build_property_from_urn_strings():
    prop = metis_data.TableProperty("my_namespace:spark:table:schema:version", "0.0.1")

    assert prop.key == "urn:my_namespace:spark:table:schema:version"
    assert prop.value == "0.0.1"


def test_build_property_from_data_agreement_type():
    prop = metis_data.TableProperty(metis_data.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace")
    assert prop.key == "urn:my_namespace:spark:table:schema:version"
    assert prop.value == "0.0.1"


def test_build_property_from_data_agreement_type_using_specific_part():
    prop = metis_data.TableProperty(key=metis_data.DataAgreementType.SCHEMA,
                                   value="0.0.1",
                                   ns="my_namespace",
                                   specific_part="myVersion:version")
    assert prop.key == "urn:my_namespace:spark:table:schema:myVersion:version"
    assert prop.value == "0.0.1"


def test_add_table_properties_on_create(di_initialise_spark,
                                        dataproduct1_ns):
    table_cls = namespaces_and_tables.my_table_cls()

    my_table = table_cls(namespace=dataproduct1_ns)

    my_table.try_write_append(data.my_table_df())

    props = my_table.property_manager.to_table_properties()

    assert len(props) == 1
    assert props[0].key == "urn:my_namespace:spark:table:schema:version"
    assert props[0].value == "0.0.1"


def test_find_table_props_by_urn(di_initialise_spark,
                                 dataproduct1_ns):
    table_cls = namespaces_and_tables.my_table_with_props_cls()

    my_table = table_cls(namespace=dataproduct1_ns)

    props = my_table.property_manager.to_table_properties()

    assert len(props) == 6
    expected_keys = ['urn:my_namespace:catalogue:description', 'urn:my_namespace:dataProduct:port',
                     'urn:my_namespace:dq:updateFrequency', 'urn:my_namespace:spark:table:schema:partitionColumns',
                     'urn:my_namespace:spark:table:schema:pruneColumn', 'urn:my_namespace:spark:table:schema:version']

    assert [p.key for p in props] == expected_keys


def test_doesnt_add_table_props_when_none_defined(di_initialise_spark,
                                                  dataproduct1_ns):
    table_cls = namespaces_and_tables.my_table2_cls()

    my_table2 = table_cls(namespace=dataproduct1_ns)

    my_table2.try_write_append(data.my_table_2_df())

    props = my_table2.property_manager.to_table_properties()

    assert not props
