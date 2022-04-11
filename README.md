# Overview
This project contains two modules that can help you work with AWS IoT TwinMaker if you have your entity data stored in snowflake: snowflake sync connector and snowflake data connector.

# Snowflake Sync Connector: Migrating Snowflake Assets to IoT TwinMaker 
## Summary
With this Snowflake module, you can extract asset information from snowflake database and store it in S3 as JSON file. This json file can then be imported into IoT TwinMaker as entities. The module allows you to extract data using your SQL query, so you can define the hierarchy of the model via the SQL query. This migration can be achieved either by manually executing the export and import scripts or by executing the step function that is created when you deploy this module as CDK.

![Architecture Flow](snowflake_workflow.jpg)

## Prerequisite
Check out the latest code from https://github.com/aws-samples/aws-iot-twinmaker-snowflake. Let's call this directory "IoTTwinMakerHome."

Create a secret in secret managerwith the following key (used to connect to snowflake);

USER
PASSWORD
ACCOUNT
ROLE
WAREHOUSE
DATABASE
SCHEMA


## Execute as stand-alone script
```
export IoTTwinMakerHome=<Directory where your checked out the code>
export PYTHONPATH=.:${IoTTwinMakerHome}/src/modules/snowflake/sync-connector-lambda:${IoTTwinMakerHome}/src/modules/snowflake/snowflake-python-and-boto3/python:$PYTHONPATH
# where IoTTwinMakerHome is the directory where you checked out the code.
```

### To export the snowflake models and assets from iot snowflake to S3
```
cd ${IoTTwinMakerHome}/src/modules/snowflake/
python migration.py
    -b  --bucket                    The bucket to exported snowflake artifacts to.
                                    e.g. my-tmp-east
    -p  --prefix                    The prefix under which snowflake data will be exported to.
                                    e.g. sf-connectors/sf-export
    -s  --secrets-name              Name of the secret in secret manager that stores snowflake credentials
                                    e.g. sf_conn_params
    -f  --query-file                File containing the query that will be executed against the snowflake data
                                    e.g. sync-connector-lambda/sf_qry.sql
    #-r  --iottwinmaker-role-arn     ARN of the role assumed by Iottwinmaker
    -w  --workspace-id              Workspace id passed to import, optional for export
                                    e.g. sf-ws
    -c  --component-type-id         Component to use
                                    e.g. sf-component

```

## Execute as step function
### Deploy the module using CDK
Check out the latest code from https://github.com/aws-samples/aws-iot-twinmaker-samples.
Deploy with cdk from the snowflake module directory as shown in the following.
```
cd cdk && cdk synth && cdk bootstrap aws://unknown-account/us-east-1 && cdk deploy
```
Execute the step function with the following input. (or schedule it in the event rule that is created as part of the cdk deployment
```
{
    "bucket": "my-tmp-east",
    "entity_prefix": "my-namespace-",
    "prefix": "snowflake/exports",
    "workspace_id": "snowflake",
    "iottwinmaker_role_arn": "arn:aws:iam::00000000000:role/iot-tm-service-role"
}
```

where
```
    bucket                  The bucket containing exported snowflake models
    entity_prefix           Prefix to namespace entities
    prefix                  The prefix to store exported snowflake assets and models
    workspace_id            Workspace id that will be created
    iottwinmaker_role_arn   IAM role that has permissions to create a workspace
```

## Sample execution screenshots
![snowflake_data](snowflake_dataset.jpg)

![snowflake_stepfunction](snowflake_stepfunction.jpg)

![snowflake_twinmaker](snowflake_twinmaker.jpg)

---

# Snowflake Data Connector: Make IoT TwinMaker connect to Snowflake

## Summary 
With the Snowflake data connector module, you can deploy several types of data connectors to your account, which you can register to TwinMaker [Component Type](https://docs.aws.amazon.com/iot-twinmaker/latest/guide/twinmaker-component-types.html). Once set up, TwinMaker can invoke those data connectors to fetch data from Snowflake in runtime. Connectors are written as Lambda functions and deployed using [AWS SAM](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-getting-started.html). 

## Connectors
Connectors included in the module are:
1. `SchemaInitializer`: a control plane connector used in the entity lifecycle to import component properties from the snowflake.
2. `DataReaderByEntity`: a data plane connector used to fetch the time-series values of properties within a single component.
3. `DataReaderByComponentType`: a data plane connector used to fetch the time-series values of properties that inherit from the same component type.
4. `AttributePropertyValueReaderByEntity`: a data plane connector used to fetch the value of static properties within a single component.
5. `DataWriter`: a data plane connector used to write time-series data points back to snowflake for properties within a single component.

## Prerequisite
The connectors get snowflake credentials from AWS Secret. In `src/modules/snowflake/data-connector/template.yaml` file, fill in your snowflake credentials into the AWS Secret SAM template.

## Deploy to your account
The Serverless Application Model Command Line Interface (SAM CLI) is an extension of the AWS CLI that adds functionality for building and testing Lambda applications. It uses Docker to run your functions in an Amazon Linux environment that matches Lambda. It can also emulate your application's build environment and API.

To use the SAM CLI, you need the following tools.

* SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* [Python 3 installed](https://www.python.org/downloads/)
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community)

To build and deploy your application for the first time, follow below steps:
1. Export secret token from your target AWS account;
2. Run below shell commands:
```bash
sam build --use-container
sam deploy --guided
```
The first command will build the source of your application. The second command will package and deploy your application to AWS, with a series of prompts:

* **Stack Name**: The name of the stack to deploy to CloudFormation. This should be unique to your account and region, and a good starting point would be something matching your project name.
* **AWS Region**: The AWS region you want to deploy your app to.
* **Confirm changes before deploy**: If set to yes, any change sets will be shown to you before execution for manual review. If set to no, the AWS SAM CLI will automatically deploy application changes.
* **Allow SAM CLI IAM role creation**: Many AWS SAM templates, including this example, create AWS IAM roles required for the AWS Lambda function(s) included to access AWS services. By default, these are scoped down to minimum required permissions. To deploy an AWS CloudFormation stack which creates or modifies IAM roles, the `CAPABILITY_IAM` value for `capabilities` must be provided. If permission isn't provided through this prompt, to deploy this example you must explicitly pass `--capabilities CAPABILITY_IAM` to the `sam deploy` command.
* **Save arguments to samconfig.toml**: If set to yes, your choices will be saved to a configuration file inside the project, so that in the future you can just re-run `sam deploy` without parameters to deploy changes to your application. Typically, choose no.

You can find your lambda and its ARN in the Outputs displayed after deployment.

### Use the SAM CLI to build and test locally
1. Export secret token from your target AWS account;
2. Build your application with the `sam build --use-container` command.
```bash
SnowflakeConnector$ sam build --use-container
```
The SAM CLI installs dependencies defined in `lambda_connectors/requirements.txt`, creates a deployment package, and saves it in the `.aws-sam/build` folder.
3. Test a single function by invoking it directly with a test event. An event is a JSON document that represents the input that the function receives from the event source.
```bash
SnowflakeConnector$ sam local invoke SnowflakeSchemaInitializer --event <your_input_json_event_file_path>
```
NOTE: If sometimes it stuck at mounting step or invocation had timeout error, you may restart your docker and repeat it again.

### Fetch, tail, and filter Lambda function logs

To simplify troubleshooting, SAM CLI has a command called `sam logs`. `sam logs` lets you fetch logs generated by your deployed Lambda function from the command line. In addition to printing the logs on the terminal, this command has several nifty features to help you quickly find the bug.

`NOTE`: This command works for all AWS Lambda functions; not just the ones you deploy using SAM.

```bash
SnowflakeConnector$ sam logs -n SnowflakeSchemaInitializer --stack-name SnowflakeConnector --tail
```

You can find more information and examples about filtering Lambda function logs in the [SAM CLI Documentation](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-logging.html).

### Cleanup

To delete the sample application that you created, use the AWS CLI. Assuming you used your project name for the stack name, you can run the following:

```bash
aws cloudformation delete-stack --stack-name SnowflakeConnector
```

### Resources

See the [AWS SAM developer guide](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html) for an introduction to SAM specification, the SAM CLI, and serverless application concepts.

Next, you can use AWS Serverless Application Repository to deploy ready to use Apps that go beyond hello world samples and learn how authors developed their applications: [AWS Serverless Application Repository main page](https://aws.amazon.com/serverless/serverlessrepo/)

## Use in Component Types
Once you deployed your lambda connectors and have the Lambda ARN ready, you can [create functions](https://docs.aws.amazon.com/iot-twinmaker/latest/guide/twinmaker-component-types.html#twinmaker-component-types-function) in the component type. You can do that in both AWS CLI and TwinMaker console. 

# License

This project is licensed under the Apache-2.0 License.
