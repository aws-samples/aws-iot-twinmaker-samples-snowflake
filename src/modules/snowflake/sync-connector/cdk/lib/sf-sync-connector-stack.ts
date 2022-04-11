// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import * as cdk from '@aws-cdk/core';
import * as events from '@aws-cdk/aws-events';
import * as targets from '@aws-cdk/aws-events-targets';
import * as iam from '@aws-cdk/aws-iam';
import * as lambda from '@aws-cdk/aws-lambda';
import * as lambdapython from "@aws-cdk/aws-lambda-python";
import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks';

import * as path from 'path';
import console = require('console');

export class SfSyncConnectorStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here
    const iottwinmaker_connector_role = new iam.Role(this, 'iottwinmaker_connector_role', {
        assumedBy: new iam.CompositePrincipal(
            new iam.ServicePrincipal('lambda.amazonaws.com'),
            new iam.ServicePrincipal('states.amazonaws.com'),
            new iam.ServicePrincipal('events.amazonaws.com'),
            new iam.ServicePrincipal('iottwinmaker.amazonaws.com'),
        ),
        managedPolicies: [
            iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
            iam.ManagedPolicy.fromAwsManagedPolicyName('CloudWatchLogsFullAccess'),
            iam.ManagedPolicy.fromAwsManagedPolicyName('AWSStepFunctionsReadOnlyAccess'),
            iam.ManagedPolicy.fromAwsManagedPolicyName('SecretsManagerReadWrite')
        ]
    });

    const policy = new iam.ManagedPolicy(this, "IoTTwinMakerFullAccessPolicy", {
        statements: [
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: ["*" ],
                resources: ["*"]
            })
        ],
        roles: [iottwinmaker_connector_role]
    });

    console.log("PWD:" + __dirname)
    console.log(path.join(__dirname, '..','..', 'sync-connector-lambda'))
    const iottwinmaker_env = new lambda.LayerVersion(this, 'iottwinmaker_env', {
        code: lambda.Code.fromAsset(path.join(__dirname, '..', '..', 'snowflake-python-and-boto3')),
        //compatibleRuntimes: [lambda.Runtime.PYTHON_3_9, lambda.Runtime.PYTHON_3_8, lambda.Runtime.PYTHON_3_7]
        compatibleRuntimes: [lambda.Runtime.PYTHON_3_8]
    });

    /*****************************************************/
    /* Snowflake sync connector importer lambda function */
    /*****************************************************/
    const iottwinmaker_importer = new lambda.Function(this, 'iottwinmakerImporterLambda', {
        code: lambda.Code.fromAsset(path.join(__dirname, '..','..', 'sync-connector-lambda')),
        handler: this.node.tryGetContext('IottwinmakerImporterHandler'),
        memorySize: 256,
        role: iottwinmaker_connector_role,
        runtime: lambda.Runtime.PYTHON_3_8,
        timeout: cdk.Duration.minutes(15),
      	layers: [iottwinmaker_env]
    });

    /*****************************************************/
    /* Snowflake sync connector exporter lambda function */
    /*****************************************************/
    const snowflake_exporter_lambda = new lambdapython.PythonFunction(this, 'snowflake_exporter_lambda', {
    //const snowflake_exporter_lambda = new lambda.Function(this, 'snowflakeExporterLambda', {
      //code: lambda.Code.fromAsset(path.join(__dirname, '..','..', 'sync-connector-lambda')),
      //handler: this.node.tryGetContext('SnowflakeExporterHandler'),
      entry: path.join(__dirname, '..','..', 'sync-connector-lambda'),
      handler: this.node.tryGetContext('SnowflakeExporterHandler').split(".")[1],
      index: this.node.tryGetContext('SnowflakeExporterHandler').split(".")[0] + ".py",
      memorySize: 256,
      role: iottwinmaker_connector_role,
      runtime: lambda.Runtime.PYTHON_3_8,
      timeout: cdk.Duration.minutes(15),
      environment: {
        'S3_QUERY_FILE': this.node.tryGetContext('QueryFileKey'),
        'SECRET_MANAGER_SECRET': this.node.tryGetContext('SecretName'),
        'WORKSPACE_ID': this.node.tryGetContext('SnowflakeWorkspaceID')
      },
      layers: [iottwinmaker_env]
    });

    const snowflake_export_task = new tasks.LambdaInvoke(this, 'snowflake_export', {
      lambdaFunction: snowflake_exporter_lambda,
      outputPath: '$.Payload'
    });
    const snowflake_import_task = new tasks.LambdaInvoke(this, 'snowflake_import', {
      lambdaFunction: iottwinmaker_importer
    });
    const snowflake_sfn_defn = snowflake_export_task.next(snowflake_import_task);
    const snowflake_sfn = new sfn.StateMachine(this, 'snowflake_to_iottwinmaker', {
      definition: snowflake_sfn_defn
    });

    const snowflake_load_sfn_rule = new events.Rule(this, 'snowflake_load_sfn_s3_trigger', {
      schedule: events.Schedule.cron({ minute: '39' })
    });

    snowflake_load_sfn_rule.addTarget(new targets.SfnStateMachine(snowflake_sfn, {
      input: events.RuleTargetInput.fromObject({
        secretsName: this.node.tryGetContext('SecretName'),
        queryFile: this.node.tryGetContext('QueryFileKey'),
        bucket: this.node.tryGetContext('OutputBucket'),
        prefix: this.node.tryGetContext('OutputPrefix'),
        workspaceId: this.node.tryGetContext('SnowflakeWorkspaceID'),
	componentTypeId: this.node.tryGetContext('ComponentTypeID'),
        iottwinmakerRoleArn: iottwinmaker_connector_role.roleArn
      }),
    }));

  }
}
