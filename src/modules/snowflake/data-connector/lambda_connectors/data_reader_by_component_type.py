import json
import logging
from datetime import datetime
from utils import parse_next_token, udqw_constants
from utils.connection_utils import connect_snowflake
from utils.param_parser import UDQWParamsParser
from utils.param_validator import UDQParamsValidator

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Establish Snowflake connection
SNOWFLAKE_CONNECTION = connect_snowflake()

DEFAULT_ALARM_TABLE = 'TEST_ALARMS'


def post_process(dict_values, entity_id=None, component_name=None, selected_properties=None):
    result = {'propertyValues': []}
    for key in dict_values:
        entry = {}
        if entity_id and component_name:
            entry['entityPropertyReference'] = {
                'entityId': entity_id,
                'componentName': component_name,
                'propertyName': selected_properties[0]
            }
        else:
            entry['entityPropertyReference'] = {
                'externalIdProperty': {
                    'alarm_key': key
                },
                'propertyName': selected_properties[0]
            }
        entry['values'] = dict_values[key] or []
        result['propertyValues'].append(entry)

    return result


def lambda_handler(event, context):
    LOGGER.info('Event: %s', event)

    # prepare params
    validator = UDQParamsValidator()
    try:
        validator.validate(event)
    except Exception as e:
        LOGGER.error("Validation exception: %s", e)
        raise e

    param_parser = UDQWParamsParser(event)

    entity_id = param_parser.get_entity_id()
    component_name = param_parser.get_component_name()
    selected_properties = param_parser.get_selected_properties()
    alarm_status_filter = param_parser.get_alarm_filter_status()
    start_time = param_parser.get_start_time()
    LOGGER.info("Start time is %s", start_time)
    end_time = param_parser.get_end_time()
    LOGGER.info("End date time is %s", end_time)
    next_token = parse_next_token(param_parser.get_next_token(), selected_properties)
    max_results = param_parser.get_max_results()
    order_by = param_parser.get_order_by()
    alarm_id = param_parser.get_alarm_id()
    table = param_parser.get_table_name() or DEFAULT_ALARM_TABLE

    # last_date_time_operator is constructed in the local, with ['<', '<='] only
    last_date_time_operator = '<='  # handle the 1-off case when orderBy == DESC

    if next_token and len(next_token) >= 1:
        if udqw_constants.FILTER_ALARM_PROPERTY_NAME not in next_token:
            raise Exception("Invalid token {}".format(next_token))

        pagination_token = next_token[udqw_constants.FILTER_ALARM_PROPERTY_NAME]
        if order_by == udqw_constants.ORDER_BY_ASC:
            start_time = pagination_token
        else:
            end_time = pagination_token
            last_date_time_operator = '<'

    if alarm_id:
        query_string = ("SELECT ALARM_ID, EVENT_TIME, identifier(?) FROM identifier(?) " \
                + " WHERE ALARM_ID=? AND EVENT_TIME > ? AND EVENT_TIME {} ? " \
                + " ORDER BY EVENT_TIME {} LIMIT ? ;") \
                .format(last_date_time_operator, udqw_constants.getOrderByWord(order_by))

        query_params = [
            selected_properties[0], 
            table,
            alarm_id,
            start_time,
            end_time,
            max_results
        ]
    elif alarm_status_filter:
        query_string = ("SELECT ALARM_ID, EVENT_TIME, identifier(?) FROM " \
                + " (SELECT ALARM_ID, EVENT_TIME, identifier(?), FIRST_VALUE(identifier(?)) OVER (PARTITION BY ALARM_ID ORDER BY EVENT_TIME DESC) last_status " \
                + " FROM identifier(?) WHERE EVENT_TIME > ? AND EVENT_TIME {} ? ORDER BY EVENT_TIME {} LIMIT ?) "  \
                + " WHERE last_status=?;").format(last_date_time_operator, udqw_constants.getOrderByWord(order_by))

        query_params = [
            selected_properties[0],
            selected_properties[0], 
            selected_properties[0],
            table,
            start_time,
            end_time,
            max_results,
            alarm_status_filter
        ]
    else:
        query_string = ("SELECT ALARM_ID, EVENT_TIME, identifier(?) FROM identifier(?)" \
                + " WHERE EVENT_TIME > ? AND EVENT_TIME {} ? ORDER BY EVENT_TIME {} LIMIT ?;") \
                .format(last_date_time_operator, udqw_constants.getOrderByWord(order_by)) 

        query_params = [
            selected_properties[0], 
            table,
            start_time,
            end_time,
            max_results 
        ]

    LOGGER.info("Query string is %s", query_string)
    values = {}

    last_timestamp = None
    count = 0
    try:
        cursor = SNOWFLAKE_CONNECTION.cursor()
        for (alarm_id, event_time, status) in cursor.execute(query_string, query_params):
            if alarm_id not in values:
                values[alarm_id] = []
            current_event = {'time': event_time.isoformat(), 'value': {'stringValue': status}}
            values[alarm_id].append(current_event)
            last_timestamp = current_event['time']
            count += 1
    except Exception as e:
        LOGGER.error("Query exception: %s", e)
        raise e
    finally:
        cursor.close()

    result = post_process(values, entity_id, component_name, selected_properties)

    if count == max_results:
        result['nextToken'] = json.dumps({
            udqw_constants.FILTER_ALARM_PROPERTY_NAME: last_timestamp
        })

    return result
