#!/usr/bin/env python

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from snowflake_export import *
from tm_importer import *

## -f as the input CSV
def parse_arguments():
  parser = argparse.ArgumentParser(
                  description='export Snowflake model meta-data to Iottwinmaker')
  parser.add_argument('-b', '--bucket',
                        help='S3 bucket to store the exported files to.',
                        required=True)
  parser.add_argument('-p', '--prefix',
                        help='prefix path within the S3 bucket',
                        required=True)
  parser.add_argument('-r', '--iottwinmaker-role-arn',
                        help='ARN of the role assumed by Iottwinmaker',
                        default=False,
                        required=True)
  parser.add_argument('-w', '--workspace-id',
                        help='Workspace id passed to import, (optional for export)',
                        required=True)
  parser.add_argument('-s', '--secrets-name',
                        help='Name of the secret in secret manager that stores snowflake credentials',
                        required=True)
  parser.add_argument('-f', '--query-file',
                        help='File containing the query that will be executed against the snowflake data',
                        required=True)
  parser.add_argument('-n', '--entity-name-prefix',
                        help='prefix to namespace entity to avoid clash',
                        required=True)
  parser.add_argument('-c', '--component-type-id',
                        help='Component name for snowflake data',
                        required=True)
  return parser


def main():
    parser = parse_arguments()
    args = parser.parse_args()

    print("Exporting assets and models from SiteWise...")

    r = lambda_handler( {
            'secretsName': args.secrets_name,
            'queryFile': args.query_file,
            'bucket':args.bucket,
            'prefix':args.prefix,
            'workspaceId':args.workspace_id},None)
            #'iottwinmakerRoleArn':args.iottwinmaker_role_arn},None)
    print(r)

    e_key = r.get('body').get('outputPath')
    print("Importing assets and models to IoT TwinMaker...")

    import_handler( {'body':{
        'outputBucket':args.bucket,
        'outputPath':e_key,
        'workspaceId':args.workspace_id,
        'componentTypeId':args.component_type_id,
        'iottwinmakerRoleArn' : args.iottwinmaker_role_arn}}, None)

main()
