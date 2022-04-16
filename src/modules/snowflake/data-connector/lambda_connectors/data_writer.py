# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2021
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import defaultdict
from datetime import datetime
from utils import udqw_constants
from utils.bulk_loader import SnowflakeBulkLoader
from utils.param_parser import UDQWParamsParser
from utils.udw_param_validator import UDWParamsValidator

TIMESERIES_TABLE_FIELDNAMES = ['PT_ID', 'PT', 'DESCRIPTION', 'TS', 'UOM', 'DATA_TYPE', 'PT_VALUE',
                               'PT_VALUE_STR', 'PT_STATUS', 'YEAR', 'MONTH', 'DAY']
INTERNAL_STAGE_NAME = 'twinmaker_batch_write_stage'
PUT_STAGE_FILE_FORMAT_NAME = 'twinmaker_batch_write_format'
CSV_FILE_PATH_TEMPLATE = '/tmp/batch-entry-{}.csv'

# Configure logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

SNOWFLAKE_BULK_LOADER = SnowflakeBulkLoader(CSV_FILE_PATH_TEMPLATE, TIMESERIES_TABLE_FIELDNAMES,
                                            INTERNAL_STAGE_NAME, PUT_STAGE_FILE_FORMAT_NAME)


# ---------------------------------------------------------------------------
#   Sample implementation of an AWS IoT TwinMaker UDW Connector against Snowflake
#   write time-series values of multiple properties
# ---------------------------------------------------------------------------


def lambda_handler(event, context):
    LOGGER.info('Event: %s', event)

    validator = UDWParamsValidator()
    validator.validate(event)

    # 1. Parse input parameter
    param_parser = UDQWParamsParser(event)
    workspace_id = param_parser.get_workspace_id()
    properties = param_parser.get_properties()
    entries = param_parser.get_entries()

    error_entries = list()
    # 2 Prepare and generate batch data for Snowflake bulk loading
    batch_entry_list = generate_batch_entry_for_csv_file(entries, properties)
    for batch_entry in batch_entry_list:
        for table_name in batch_entry:
            bulk_data = batch_entry[table_name]
            error_entry = loading_bulk_data_into_snowflake(table_name, bulk_data, entries)
            error_entries.extend(error_entry)
    return {
        'errorEntries': error_entries
    }


def loading_bulk_data_into_snowflake(table_name, bulk_data, entries):
    # 1. Create local csv file from request
    file_path, total_rows = SNOWFLAKE_BULK_LOADER.write_csv_file(bulk_data)

    # 2. Put csv file into Snowflake internal stage
    put_status, target_file = SNOWFLAKE_BULK_LOADER.put_file_into_stage(file_path)
    if put_status != SnowflakeBulkLoader.PUT_SUCCESS_STATUS:
        # return full entries if PUT command failed
        LOGGER.error('Failed to load %s of rows to Snowflake table.', len(bulk_data))
        return handle_errors(bulk_data, len(bulk_data), 0, entries)

    # 3. Bulk load into to Snowflake table
    copy_into_status, num_errors_seen, first_error_row = SNOWFLAKE_BULK_LOADER.copy_staged_file_into_table(target_file,
                                                                                                           table_name,
                                                                                                           total_rows)

    # 4. Remove the successfully loaded data files from the stage
    SNOWFLAKE_BULK_LOADER.remove_staged_file()

    # 5. Remove the temporary file stored in Lambda function
    SNOWFLAKE_BULK_LOADER.remove_local_file(file_path)
    if copy_into_status != SnowflakeBulkLoader.COPY_INTO_SUCCESS_STATUS:
        LOGGER.error('Failed to load %s of rows to Snowflake table.', num_errors_seen)
        return handle_errors(bulk_data, num_errors_seen, first_error_row, entries)

    return list()


def generate_batch_entry_for_csv_file(entries, properties):
    batch_entry_list = list()
    for entry in entries:
        entity_property_reference = entry[udqw_constants.ENTITY_PROPERTY_REFERENCE]
        entity_id = entity_property_reference[udqw_constants.ENTITY_ID]
        property_name = entity_property_reference[udqw_constants.PROPERTY_NAME]
        property_foreign_key = properties[entity_id][property_name]['definition'][udqw_constants.CONFIGURATION]['PT']
        table_name = properties[entity_id][udqw_constants.TIMESERIES_TABLE_NAME]['value']['stringValue']
        property_values = entry[udqw_constants.PROPERTY_VALUES]
        entry_id = entry[udqw_constants.ENTRY_ID]
        data_records = defaultdict(list)
        for property_value in property_values:
            timestamp = property_value[udqw_constants.TIMESTAMP]
            epoch_time = datetime.fromisoformat(timestamp).timestamp()
            value = property_value[udqw_constants.PROPERTY_VALUE]['doubleValue']
            data_record = {
                'PT': property_foreign_key,
                'TS': timestamp,
                'PT_VALUE': value,
                'EPOCH_TIME': epoch_time,
                'ENTRY_ID': entry_id
            }
            data_records[table_name].append(data_record)
        batch_entry_list.append(data_records)
    return batch_entry_list


def handle_errors(batch_entry, num_errors_seen, first_error_row, entries):
    error_entries = batch_entry[first_error_row: first_error_row + num_errors_seen]
    error_entry_map = defaultdict(list)
    for row in error_entries:
        entry_id, epoch_time = row['ENTRY_ID'], row['EPOCH_TIME']
        error_entry_map[entry_id].append(epoch_time)
    return generate_property_error_entries(entries, error_entry_map)


def generate_property_error_entries(entries, error_entry_map):
    error_entries = list()
    for entry in entries:
        entry_id = entry[udqw_constants.ENTRY_ID]
        entity_property_reference = entry[udqw_constants.ENTITY_PROPERTY_REFERENCE]
        property_values = entry[udqw_constants.PROPERTY_VALUES]

        error_entry = defaultdict(list)
        if entry_id in error_entry_map:
            error_entry[udqw_constants.ERROR_CODE] = udqw_constants.UDW_DEFAULT_ERROR_CODE
            error_entry[udqw_constants.ERROR_MESSAGE] = udqw_constants.UDW_DEFAULT_ERROR_MESSAGE
            error_entry[udqw_constants.ENTRY] = defaultdict(list)
            error_entry[udqw_constants.ENTRY][udqw_constants.ENTRY_ID] = entry_id
            error_entry[udqw_constants.ENTRY][udqw_constants.ENTITY_PROPERTY_REFERENCE] = entity_property_reference

            error_entry_values = error_entry_map[entry_id]
            for property_value in property_values:
                timestamp = property_value[udqw_constants.TIMESTAMP]
                if timestamp in error_entry_values:
                    error_entry[udqw_constants.ENTRY][udqw_constants.PROPERTY_VALUES].append(property_value)
            error_entries.append({'errors': [error_entry]})
    return error_entries
