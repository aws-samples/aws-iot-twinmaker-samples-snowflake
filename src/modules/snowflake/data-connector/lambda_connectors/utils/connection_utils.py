# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2021
# SPDX-License-Identifier: Apache-2.0

import json
import boto3
import snowflake.connector

SNOWFLAKE_SECRET_ID = 'SnowflakeSecret'
SNOWFLAKE_SECRET_KEY_ACCOUNT = 'ACCOUNT'
SNOWFLAKE_SECRET_KEY_USER = 'USER'
SNOWFLAKE_SECRET_KEY_PASSWORD = 'PASSWORD'
SNOWFLAKE_SECRET_KEY_ROLE = 'ROLE'
SNOWFLAKE_SECRET_KEY_WAREHOUSE = 'WAREHOUSE'
SNOWFLAKE_SECRET_KEY_DATABASE = 'DATABASE'
SNOWFLAKE_SECRET_KEY_SCHEMA = 'SCHEMA'


def connect_snowflake():

    # Fetch Snowflake credential
    secret = load_secret(SNOWFLAKE_SECRET_ID)

    snowflake.connector.paramstyle='qmark'
    
    # Establish Snowflake connection
    return snowflake.connector.connect(
        account=secret[SNOWFLAKE_SECRET_KEY_ACCOUNT],
        user=secret[SNOWFLAKE_SECRET_KEY_USER],
        password=secret[SNOWFLAKE_SECRET_KEY_PASSWORD],
        role=secret[SNOWFLAKE_SECRET_KEY_ROLE],
        warehouse=secret[SNOWFLAKE_SECRET_KEY_WAREHOUSE],
        database=secret[SNOWFLAKE_SECRET_KEY_DATABASE],
        schema=secret[SNOWFLAKE_SECRET_KEY_SCHEMA]
    )


def load_secret(secret_id):

    aws_session = boto3.session.Session()
    secrets_manager_client = aws_session.client(
        service_name='secretsmanager'
    )

    get_secret_value_response = secrets_manager_client.get_secret_value(
        SecretId=secret_id
    )

    return json.loads(get_secret_value_response['SecretString'])
