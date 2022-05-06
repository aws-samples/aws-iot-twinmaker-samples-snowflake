# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2021
# SPDX-License-Identifier: Apache-2.0

import os
import csv
import logging
import uuid
from utils.connection_utils import connect_snowflake

# Configure logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Connect to Snowflake
SNOWFLAKE_CONNECTION = connect_snowflake()


class SnowflakeBulkLoader:
    # Snowflake status
    PUT_SUCCESS_STATUS = 'UPLOADED'
    COPY_INTO_SUCCESS_STATUS = 'LOADED'

    def __init__(self, file_path_template, field_names, stage_name, file_format_name):
        self.file_path_template = file_path_template
        self.field_names = field_names
        self.__schema_set = set(field_names)
        self.stage_name = stage_name
        self.file_format_name = file_format_name

    def __schema_filter(self, entry):
        row = dict()
        for column_name, value in entry.items():
            if column_name in self.__schema_set:
                row[column_name] = value
        return row

    """
    Lambda /tmp directory size limit is 512MB, may improve/check this later.
    Reference: https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html
    """

    def write_csv_file(self, batch_entry):
        file_path = self.file_path_template.format(uuid.uuid4())
        total_rows = 0
        with open(file_path, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.field_names)
            for row_entry in batch_entry:
                # only write necessary columns based on Snowflake table schema
                writer.writerow(self.__schema_filter(row_entry))
                total_rows += 1
        LOGGER.info("Created a temporary CSV file in {}".format(file_path))
        return file_path, total_rows

    def put_file_into_stage(self, file_path):
        status = target_file = None
        cursor = SNOWFLAKE_CONNECTION.cursor()
        try:
            put_result = cursor.execute(
                "PUT file://{} @{} AUTO_COMPRESS=TRUE;".format(file_path,
                                                               self.stage_name)).fetchone()
            target_file, status = put_result[1], put_result[6]
            LOGGER.info(
                '{} PUT file://{} into Snowflake stage {} as {}.'.format(status, file_path, self.stage_name,
                                                                         target_file))
        except Exception as e:
            LOGGER.error("PUT file exception: {}".format(e))
        finally:
            cursor.close()

        return status, target_file

    def copy_staged_file_into_table(self, target_file, table_name, total_rows):
        """
        COPY INTO command output reference:
        https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#output
        """
        status = SnowflakeBulkLoader.COPY_INTO_SUCCESS_STATUS
        num_errors_seen = total_rows
        first_error_row = 0 

        # Load data from staged files into an existing table using COPY INTO command
        cursor = SNOWFLAKE_CONNECTION.cursor()
        try:
            copy_into_result = cursor.execute(
                'COPY INTO {} FROM @{}/{} FILE_FORMAT = (FORMAT_NAME = {});'.format(table_name,
                                                                                    self.stage_name,
                                                                                    target_file,
                                                                                    self.file_format_name)).fetchone()

            LOGGER.info(
                "Output message from Snowflake COPY INTO command: {}".format(copy_into_result))

            if len(copy_into_result) > 1:
                status, num_errors_seen, first_error_row = copy_into_result[1], copy_into_result[5], copy_into_result[7]
                LOGGER.info(
                    "COPY INTO command status={}, num_errors_seen={}, first_error_row={}".format(
                        status, num_errors_seen, first_error_row))
                LOGGER.info('COPY {} of rows INTO to Snowflake table "{}"."'.format(copy_into_result[3], table_name))

        except Exception as e:
            LOGGER.error("Failed to COPY {} INTO {} exception: {}".format(target_file, self.stage_name, e))
        finally:
            cursor.close()
        """
        According to Snowflake doc: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#output
        first_error_row should always return a Number, but it returns a None when no errors occurred
        enforce first_error_row = 0 before return
        """
        if not first_error_row:
            first_error_row = 0

        return status, num_errors_seen, first_error_row

    def remove_staged_file(self):
        cursor = SNOWFLAKE_CONNECTION.cursor()
        try:
            for (file_name, status) in cursor.execute(
                    'REMOVE @{} PATTERN=".*.csv.gz";'.format(self.stage_name)):
                LOGGER.info('{} file name: {} from stage name: {}.'.format(status, file_name, self.stage_name))
        except Exception as e:
            LOGGER.error("Failed to execute REMOVE command, {}".format(e))
            raise e
        finally:
            cursor.close()

    @staticmethod
    def remove_local_file(file_path):
        try:
            os.remove(file_path)
            LOGGER.info("Removed file in local path: {}".format(file_path))
        except Exception as e:
            LOGGER.error("Failed to remove local file, {}".format(e))
