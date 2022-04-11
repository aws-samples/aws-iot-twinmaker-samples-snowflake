import logging
from utils.connection_utils import connect_snowflake

REQUEST_KEY_PROPERTIES = 'properties'
REQUEST_KEY_ELEM_ID = 'elemId'
REQUEST_KEY_ENTITY_PROPERTY_TABLE_NAME = 'entityPropertyTableName'
REQUEST_KEY_VALUE = 'value'
REQUEST_KEY_VALUE_STRING = 'stringValue'

ILLEGAL_CHARACTERS = ['#', '(', ')', ' ']

# Configure logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Connect to Snowflake
SNOWFLAKE_CONNECTION = connect_snowflake()


def lambda_handler(event, context):

    cursor = SNOWFLAKE_CONNECTION.cursor()
    properties = {}

    # Prepare and execute query statement to Snowflake
    elem_id_property = event[REQUEST_KEY_PROPERTIES][REQUEST_KEY_ELEM_ID]
    table_name_property = event[REQUEST_KEY_PROPERTIES][REQUEST_KEY_ENTITY_PROPERTY_TABLE_NAME]

    elem_id = elem_id_property[REQUEST_KEY_VALUE][REQUEST_KEY_VALUE_STRING] if REQUEST_KEY_VALUE in elem_id_property else elem_id_property['definition']['defaultValue'][REQUEST_KEY_VALUE_STRING]
    table_name = table_name_property[REQUEST_KEY_VALUE][REQUEST_KEY_VALUE_STRING] if REQUEST_KEY_VALUE in table_name_property else table_name_property['definition']['defaultValue'][REQUEST_KEY_VALUE_STRING]

    try:
        query = 'select ATTR_NAME, ATTR_VALUE, ATTR_PI_PT, PT_UOM, PT_DATATYPE from identifier(?) where ELEM_ID = ?;'

        LOGGER.info('elem_id: %s', elem_id)

        for (attr_name, attr_value, attr_pi_pt, pt_uom, data_type) in cursor.execute(query, [ table_name, elem_id]):
            if attr_name is None or data_type is None:
                raise ValueError('Data type and attribute name cannot be null')

            current_property = {
                'definition': {}
            }

            if pt_uom is not None:
                current_property['definition']['dataType'] = {
                    'type': map_to_roci_data_type(attr_name, data_type),
                    'unitOfMeasure': pt_uom
                }
            else:
                current_property['definition']['dataType'] = {
                    'type': map_to_roci_data_type(attr_name, data_type)
                }
        
            if attr_pi_pt is not None:
                current_property['definition']['configuration'] = {
                    'PT': attr_pi_pt
                }
                current_property['definition']['isTimeSeries'] = True
            else:
                current_property['definition']['defaultValue'] = {
                    'stringValue': attr_value
                }
                current_property['value'] = {
                    'stringValue': attr_value
                }
                current_property['definition']['isTimeSeries'] = False

            # Some characters are not allowed to present in property name
            attr_name = replace_illegal_character(attr_name)
            properties[attr_name] = current_property
    
    except Exception as e:
        LOGGER.error("Query exception: %s", e)
        raise e

    finally:
        cursor.close()

    return {
        'properties': properties
    }


def replace_illegal_character(attr_name):
    for illegal_char in ILLEGAL_CHARACTERS:
        attr_name = attr_name.replace(illegal_char, '_')
    return attr_name.replace('__', '_')


def map_to_roci_data_type(attr_name, data_type):
    """
    DATA_TYPE	Type
    -------------------
    6	        Int16
    8	        Int32
    11	        Float16
    12	        Float32
    13	        Float64
    101	        Digital
    104	        Timestamp
    105	        String
    102	        Blob
    """
    data_type_mapping = {
        6: 'INTEGER',
        8: 'INTEGER',
        11: 'DOUBLE',
        12: 'DOUBLE',
        13: 'DOUBLE',
        101: 'STRING',
        104: 'STRING',
        105: 'STRING',
        102: 'STRING'
    }

    if data_type in data_type_mapping.keys():
        return data_type_mapping[data_type]
    else:
        raise ValueError('Invalid data type {} for attribute {}'.format(str(data_type), attr_name))
