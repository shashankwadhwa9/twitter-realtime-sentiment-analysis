import os
import json
import time
import boto3
from local_settings import S3_BUCKET_NAME, KINESIS_DELIVERY_STREAM

ACCOUNT_ID = boto3.client('sts').get_caller_identity().get('Account')


def create_s3_bucket():
    client = boto3.client('s3')

    # Create bucket
    response = client.create_bucket(
        Bucket=S3_BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'}
    )

    # Block public access
    client.put_public_access_block(
        Bucket=S3_BUCKET_NAME,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        }
    )
    print(response)
    print('S3 bucket created')


def create_kinesis_delivery_stream():
    iam_client = boto3.client('iam')

    # Create role
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "firehose.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    role_name = 'kinesis_firehose_twitter_role'
    role = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(assume_role_policy_document),
        Description='S3 and Cloudwatch access for twitter kinesis delivery stream'
    )
    print('Role created')
    time.sleep(10)

    # Create policy
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET_NAME}",
                    f"arn:aws:s3:::{S3_BUCKET_NAME}/*"
                ]
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "logs:PutLogEvents"
                ],
                "Resource": "*"
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "kinesis:DescribeStream",
                    "kinesis:GetShardIterator",
                    "kinesis:GetRecords",
                    "kinesis:ListShards"
                ],
                "Resource": "*"
            }
        ]
    }
    policy = iam_client.create_policy(
        PolicyName='kinesis_firehose_twitter_role_policy',
        PolicyDocument=json.dumps(policy_document)
    )
    print('Policy created')
    time.sleep(10)

    # Attach policy to the role
    iam_client.attach_role_policy(
        PolicyArn=policy['Policy']['Arn'],
        RoleName=role_name
    )
    print('Attached policy to role')
    time.sleep(10)

    firehose_client = boto3.client('firehose')
    role_arn = role['Role']['Arn']
    max_tries = 3
    current_tries = 0
    success = False
    # Sometimes it takes time for aws to correctly identify the role, hence try a few times
    while current_tries < max_tries and success is False:
        try:
            # Create Kinesis Data Firehose delivery stream
            response = firehose_client.create_delivery_stream(
                DeliveryStreamName=KINESIS_DELIVERY_STREAM,
                DeliveryStreamType='DirectPut',
                ExtendedS3DestinationConfiguration={
                    'RoleARN': role_arn,
                    'BucketARN': f'arn:aws:s3:::{S3_BUCKET_NAME}',
                    'Prefix': '!{timestamp:yyyy/MM/dd}/',
                    'ErrorOutputPrefix': 'error',
                    'BufferingHints': {
                        'SizeInMBs': 10,
                        'IntervalInSeconds': 900
                    },
                    'CompressionFormat': 'UNCOMPRESSED',
                    'CloudWatchLoggingOptions': {
                        'Enabled': True,
                        'LogGroupName': 'kinesis-delivery-streams',
                        'LogStreamName': 'twitter'
                    }
                }
            )
            print(response)
            print('Kinesis Data Firehose delivery stream created')
            success = True
        except Exception as e:
            print(e)
            time.sleep(10)
            current_tries += 1


def package_lambda():
    """
    Packages the lambda code in a zip file
    :return: Zipped code content
    """

    lambda_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'lambda')
    commands = [
        f'cd {lambda_dir} && rm -rf lambda_env',  # Remove previously created env
        f'cd {lambda_dir} && virtualenv -p python3.6 lambda_env',  # Create new virtualenv
        f'cd {lambda_dir} && source lambda_env/bin/activate && pip install -r requirements.txt',  # Activate the env
        f'cd {lambda_dir}/lambda_env/lib/python3.6/site-packages/ && zip -r9 ../../../../lambda.zip *',
        f'cd {lambda_dir} && zip -g lambda.zip *.py'
    ]

    for cmd in commands:
        os.system(cmd)

    with open(f'{lambda_dir}/lambda.zip', 'rb') as f:
        zipped_code = f.read()

    return zipped_code


def create_lambda_function():
    iam_client = boto3.client('iam')

    # Create role
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    role_name = 'lambda_twitter_role'
    role = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(assume_role_policy_document),
        Description='S3 and ES access for twitter lambda'
    )
    print('Role created')
    time.sleep(10)

    # Create policy
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "logs:CreateLogGroup",
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "es:ESHttpPost"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": "arn:aws:s3:::*"
            }
        ]
    }
    policy = iam_client.create_policy(
        PolicyName='lambda_twitter_role_policy',
        PolicyDocument=json.dumps(policy_document)
    )
    print('Policy created')
    time.sleep(10)

    # Attach policy to the role
    iam_client.attach_role_policy(
        PolicyArn=policy['Policy']['Arn'],
        RoleName=role_name
    )
    print('Attached policy to role')
    time.sleep(10)

    client = boto3.client('lambda')
    zipped_code = package_lambda()
    role_arn = role['Role']['Arn']
    max_tries = 3
    current_tries = 0
    success = False
    # Sometimes it takes time for aws to correctly identify the role, hence try a few times
    while current_tries < max_tries and success is False:
        try:
            response = client.create_function(
                FunctionName='load-tweets-to-es',
                Runtime='python3.6',
                Role=role_arn,
                Handler='lambda_runner.lambda_handler',
                Code={
                    'ZipFile': zipped_code,
                },
                Description='Lambda function to process a tweet and push to ES',
                Timeout=900,
                MemorySize=1280
            )
            print(response)
            print('Lambda created')
            success = True
        except Exception as e:
            print(e)
            time.sleep(10)
            current_tries += 1


def add_trigger_for_s3_bucket():
    """
    Adds a trigger for S3. If any new file comes to the S3 bucket, the lambda function would get invocated.
    """
    # Give permission to S3 bucket to invoke this lambda
    lambda_client = boto3.client('lambda')
    lambda_client.add_permission(
        Action='lambda:InvokeFunction',
        FunctionName='load-tweets-to-es',
        Principal='s3.amazonaws.com',
        SourceAccount=str(ACCOUNT_ID),
        SourceArn=f'arn:aws:s3:::{S3_BUCKET_NAME}',
        StatementId='s3',
    )
    print('Resource-based policy added to lambda')
    time.sleep(20)

    # Add event trigger
    s3_client = boto3.client('s3')
    response = s3_client.put_bucket_notification_configuration(
        Bucket=S3_BUCKET_NAME,
        NotificationConfiguration={
            'LambdaFunctionConfigurations': [
                {
                    'Id': 'sw-tweets-lambda-trigger',
                    'LambdaFunctionArn': f'arn:aws:lambda:ap-south-1:{ACCOUNT_ID}:function:load-tweets-to-es',
                    'Events': [
                        's3:ObjectCreated:Put'
                    ]
                }
            ]
        }
    )
    print(response)
    print('Trigger added for S3 bucket')


def create_es_cluster():
    client = boto3.client('es')
    response = client.create_elasticsearch_domain(
        DomainName='sw-es-cluster',
        ElasticsearchVersion='7.4',
        ElasticsearchClusterConfig={
            'InstanceType': 't2.small.elasticsearch',
            'InstanceCount': 1,
            'DedicatedMasterEnabled': False,
            'ZoneAwarenessEnabled': False
        },
        EBSOptions={
            'EBSEnabled': True,
            'VolumeType': 'standard',
            'VolumeSize': 10
        }
    )
    print(response)
    print('ES cluster created')


def update_lambda_function_code():
    client = boto3.client('lambda')
    zipped_code = package_lambda()
    response = client.update_function_code(
        FunctionName='load-tweets-to-es',
        ZipFile=zipped_code,
        Publish=True,
        DryRun=False
    )
    print(response)
    print('Lambda code updated')


if __name__ == '__main__':
    # create_s3_bucket()
    # create_kinesis_delivery_stream()
    # create_lambda_function()
    # add_trigger_for_s3_bucket()
    # create_es_cluster()
    update_lambda_function_code()
