# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2021
# SPDX-License-Identifier: Apache-2.0

import logging
from utils import udqw_constants
from utils.connection_utils import connect_snowflake
from utils.param_parser import UDQWParamsParser

VALUE_TYPE_DATA_TYPE_MAPPING = {
    'DOUBLE': 'doubleValue',
    'LONG': 'longValue',
    'INTEGER': 'intValue',
    'STRING': 'stringValue',
    'BOOLEAN': 'booleanValue'
}

# Configure logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Connect to Snowflake
SNOWFLAKE_CONNECTION = connect_snowflake()

# ---------------------------------------------------------------------------
#   Sample implementation of an AWS IoT TwinMaker UDQ Connector against Snowflake
#   queries static values of multiple properties within a single component
# ---------------------------------------------------------------------------


def lambda_handler(event, context):
    """
    Query attribute value for multiple properties from Snowflake.
    Each property has single primitive value or list of primitive values.
    The connector will return results with a map containing property name and its value.
    """

    # 1. Parse input parameter
    param_parser = UDQWParamsParser(event)

    table_name = param_parser.get_attribute_property_table_name()
    entity_id = param_parser.get_entity_id()
    component_name = param_parser.get_component_name()
    selected_properties = param_parser.get_selected_properties()
    element_id = param_parser.get_element_id()
    properties = param_parser.get_properties()

    # 2. Get selected property definitions
    property_definitions = {}
    for property_name in selected_properties:
        property_type = properties[property_name][udqw_constants.PROPERTY_DEFINITION][udqw_constants.PROPERTY_DATA_TYPE][udqw_constants.PROPERTY_TYPE]
        property_definitions[property_name] = property_type

    # 3. Generate query statement
    wrapped_properties = ['identifier(\'' + property + '\')' for property in selected_properties]
    query_statement = 'select {} from identifier(?) where elem_id = ?'.format(', '.join(wrapped_properties))
    parameters = [table_name, element_id]
    LOGGER.info('query statement: %s', query_statement)
    LOGGER.info('Table: %s, element_id: %s', table_name, element_id)

    # 4. Query Snowflake and fetch result
    cursor = SNOWFLAKE_CONNECTION.cursor()
    property_values = {}
    try:
        cursor.execute(query_statement, parameters)
        records = cursor.fetchall()
        assert (len(records) <= 1), 'Greater than 1 rows is not supported!'
        for row in records:
            for i in range(len(selected_properties)):
                property_name = selected_properties[i]
                property_values[property_name] = {
                    'propertyReference': {
                        'propertyName': property_name,
                        'entityId': entity_id,
                        'componentName': component_name
                    },
                    'propertyValue': {
                        VALUE_TYPE_DATA_TYPE_MAPPING[property_definitions[property_name]]: row[i]
                    }
                }
    finally:
        cursor.close()

    # 5. Generate response
    return {
        'propertyValues': property_values
    }
