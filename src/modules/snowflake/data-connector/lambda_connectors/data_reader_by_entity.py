import json
import logging
from datetime import datetime
from utils import parse_next_token
from utils.connection_utils import connect_snowflake
from utils.param_parser import UDQWParamsParser

VALUE_TYPE_DATA_TYPE_MAPPING = {
    'DOUBLE': 'doubleValue',
    'LONG': 'longValue',
    'INTEGER': 'intValue',
    'STRING': 'stringValue',
    'BOOLEAN': 'booleanValue'
}
ORDER_BY_ASC = 'ASC'
ORDER_BY_DESC = 'DESC'

# Configure logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Connect to Snowflake
SNOWFLAKE_CONNECTION = connect_snowflake()


def lambda_handler(event, context):
    """
    Query time-series data of multiple properties from Snowflake.
    Each property will return maximum ${MAX_RESULTS} number of data points.
    The connector will return results with a next token if not all results have been returned.
    """

    LOGGER.info('request: %s', event)

    # 1. Parse input parameter
    param_parser = UDQWParamsParser(event)

    table_name = param_parser.get_timeseries_table_name()
    entity_id = param_parser.get_entity_id()
    component_name = param_parser.get_component_name()
    selected_properties = param_parser.get_selected_properties()
    properties = param_parser.get_properties()
    start_time = param_parser.get_start_time()
    end_time = param_parser.get_end_time()
    order_by = param_parser.get_order_by()
    max_results = param_parser.get_max_results()
    next_token = param_parser.get_next_token()

    # 2. Get current page query requests
    property_next_tokens = parse_next_token(next_token, selected_properties)
    current_page_properties = {}

    if property_next_tokens is None:
        # query without next token
        for property_name in selected_properties:
            property_foreign_key = properties[property_name]['definition']['configuration']['PT']
            property_type = properties[property_name]['definition']['dataType']['type']

            property_query_start_key = None
            if order_by == ORDER_BY_ASC:
                property_query_start_key = start_time
            elif order_by == ORDER_BY_DESC:
                property_query_start_key = end_time
            else:
                raise ValueError('Invalid order {}'.format(order_by))

            current_page_properties[property_foreign_key] = \
                (property_name, property_foreign_key, property_type, property_query_start_key)

    else:
        # query with next token
        for property_name, last_exclusive_timestamp in property_next_tokens.items():
            property_foreign_key = properties[property_name]['definition']['configuration']['PT']
            property_type = properties[property_name]['definition']['dataType']['type']
            property_query_start_key = last_exclusive_timestamp
            current_page_properties[property_foreign_key] = \
                (property_name, property_foreign_key, property_type, property_query_start_key)

    # 3. Generate query statement
    statements = []
    for property_tuple in current_page_properties.values():
        statements.append(
            generate_single_property_query_statement(table_name, property_tuple, start_time, end_time,
                                                     order_by, max_results))

    # 4. Query Snowflake
    cursor = SNOWFLAKE_CONNECTION.cursor()

    property_values = {}
    for property_name in selected_properties:
        property_values[property_name] = []

    try:
        for (query, parameters) in statements:
            for (pt, value, timestamp) in cursor.execute(query, parameters):
                if pt is not None and value is not None and timestamp is not None:
                    (property_name, property_foreign_key, property_type, property_query_start_key) = current_page_properties[pt]
                    value_type = get_value_type(property_type)
                    property_values[property_name].append({
                        'time': timestamp.isoformat(),
                        'value': {
                            value_type: value
                        }
                    })
    finally:
        cursor.close()

    # 5. generate response and next token
    response_values = []
    response_token = {}

    for (property_name, values) in property_values.items():
        response_values.append({
            'entityPropertyReference': {
                'entityId': entity_id,
                'componentName': component_name,
                'propertyName': property_name
            },
            'values': values
        })
        if len(values) == max_results:
            response_token[property_name] = values[-1]['time']

    if len(response_token.keys()) > 0:
        return {
            'propertyValues': response_values,
            'nextToken': json.dumps(response_token)
        }
    else:
        return {
            'propertyValues': response_values
        }


def generate_single_property_query_statement(table_name, property_tuple, start_time, end_time, order_by, max_results):
    property_foreign_key = property_tuple[1]
    property_query_start_key = property_tuple[3]

    if order_by == ORDER_BY_ASC:
        query = 'select PT, PT_VALUE, TS from identifier(?) where PT = ? and TS > ? and TS < ? order by TS ASC LIMIT ?'
        parameters = [
            table_name, 
            property_foreign_key, 
            property_query_start_key, 
            end_time, max_results
        ]
    elif order_by == ORDER_BY_DESC:
        query = 'select PT, PT_VALUE, TS from identifier(?) where PT = ? and TS > ? and TS < ? order by TS DESC LIMIT ?'
        parameters = [
            table_name, 
            property_foreign_key, 
            start_time, 
            property_query_start_key, 
            max_results
        ]
    else:
        raise ValueError('Invalid order {}'.format(order_by))

    return (query, parameters)


def get_value_type(data_type):
    return VALUE_TYPE_DATA_TYPE_MAPPING[data_type.upper()]
