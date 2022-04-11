#!/usr/bin/env python

import argparse
import json
import os
import snowflake.connector
from snowflake.connector import DictCursor
import time

from library import *

'''
Read the model meta-data (queried from snowflake) and convert it into a Iottwinmaker entities (json)
input:
    -b  --bucket            The bucket to exported snowflake artifacts to.
    -p  --prefix            The prefix under which snowflake data will be exported to.
    -s  --secrets-name      Name of the secret in secret manager that stores snowflake credentials
    -f  --query-file        File containing the query that will be executed against the snowflake data
    #-r  --iottwinmaker-role-arn     ARN of the role assumed by Iottwinmaker
    -w  --workspace-id      Workspace id passed to import, optional for export

output:
    Spits out Iottwinmaker entity json for all records
'''

## -w Workspace ID
def parse_arguments():
  parser = argparse.ArgumentParser(
                  description='Convert OSI PI (SQL Records) to Iottwinmaker Entities (JSON)')
  parser.add_argument('-b', '--bucket',
                        help='S3 bucket to store the exported files to.',
                        required=True)
  parser.add_argument('-p', '--prefix',
                        help='prefix path within the S3 bucket',
                        required=True)
  parser.add_argument('-s', '--secrets-name',
                        help='Name of the secret in secret manager that stores snowflake credentials',
                        required=True)
  parser.add_argument('-f', '--query-file',
                        help='File containing the query that will be executed against the snowflake database',
                        required=True)
  #parser.add_argument('-r', '--iottwinmaker-role-arn',
  #                      help='ARN of the role assumed by Iottwinmaker',
  #                      default=False,
  #                      required=True)
  parser.add_argument('-w', '--workspace-id',
                        help='Workspace id passed to import, optional for export',
                        required=False)
  return parser

## upsert the entities in the workspace with that from the snowflake records
def process_records(sf_records):
    jrec = {"entities":[]}
    entities = jrec["entities"]
    for row_tuple in sf_records:
        attributes = json.loads(row_tuple['ATTR_NAME'])
        values = json.loads(row_tuple['ATTR_PI_PT'])
        properties = {}
        for i, attr in enumerate(attributes):
            ## Temporary if condition, remove it once snowflake query is fixed.
            if i < len(values):
                value = underscored(values[i]) if i < len(values) else ""
                properties[underscored(attr)] = {
                    'definition': { 'dataType': {'type':'STRING'} },
                    'value' : {'stringValue': value}
                }
        entity = {  "entity_name": underscored(row_tuple['ELEM_NAME']),
                    "entity_id":  underscored(row_tuple['ELEM_ID']),
                    "parent_name":  underscored(row_tuple.get('PARENT_NAME')),
                    "parent_entity_id": underscored(row_tuple.get('ELEM_PARENT_ID')),
                    "component_type": row_tuple.get('COMP_TYPE'),
                    "description": row_tuple.get('EPATH'),
                    "properties": properties }

        entities.append(entity)
    return jrec


def query_records(secret, qry_file):
    records = []
    sf_creds = get_snowflake_credentials(secret)
    ctx = snowflake.connector.connect(
        user=sf_creds['USER'],
        password=sf_creds['PASSWORD'],
        account=sf_creds['ACCOUNT'],
        ROLE=sf_creds['ROLE'],
        WAREHOUSE=sf_creds['WAREHOUSE'],
        DATABASE=sf_creds['DATABASE'],
        SCHEMA=sf_creds['SCHEMA']
        )

    cs = ctx.cursor(DictCursor)
    try:
        with open(qry_file, 'r') as sql:
            qry = sql.read().replace('\n', '')
        cs.execute('use warehouse ' + sf_creds['WAREHOUSE'])
        cs.execute(qry)
        records = cs.fetchall()
    finally:
        cs.close()
    ctx.close()
    return records


def lambda_handler(event, context):
    #load_env()
    secrets_name = event.get('secretsName')
    query_file = event.get('queryFile')
    records = query_records(secrets_name, query_file)
    
    SERVICE_ENDPOINT= os.environ.get('AWS_ENDPOINT')
    
    s3 = boto3_session().resource('s3')
    ws_bucket = event.get('bucket')
    filename = '{}/{}.json'.format(event.get('prefix'),str(time.time()))
    s3object = s3.Object(ws_bucket, filename)

    json_data = process_records(records)
    s3object.put(
        Body=(bytes(json.dumps(json_data).encode('UTF-8')))
    )
    
    return  {
        'statusCode': 200,
        'body': {   'outputBucket': ws_bucket,
                    'outputPath':filename,
                    'componentTypeId':'com.snowflake.connector:1',
                    'workspaceId':event.get('workspaceId')},
                    #'iottwinmakerRoleArn':event.get('iottwinmakerRoleArn')},
        'status' : "SUCCEEDED"
    }


def main():
    if __name__ != '__main__':
        return
    parser = parse_arguments()
    args = parser.parse_args()

    r = lambda_handler( {
            'secretsName': args.secrets_name,
            'queryFile': args.query_file,
            'bucket':args.bucket,
            'prefix':args.prefix,
            'workspaceId':args.workspace_id},None)
            #'iottwinmakerRoleArn':args.iottwinmaker_role_arn},None)
    print(r)

main()

