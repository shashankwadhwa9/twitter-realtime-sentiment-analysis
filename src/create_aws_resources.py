import os
import json
import time
import boto3
from local_settings import S3_BUCKET_NAME, KINESIS_DELIVERY_STREAM


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

    max_tries = 3
    current_tries = 0
    success = False
    # Sometimes it takes time for aws to correctly identify the role, hence try a few times
    while current_tries < max_tries and success is False:
        try:
            # Create Kinesis Data Firehose delivery stream
            firehose_client = boto3.client('firehose')
            role_arn = role['Role']['Arn']
            response = firehose_client.create_delivery_stream(
                DeliveryStreamName=KINESIS_DELIVERY_STREAM,
                DeliveryStreamType='DirectPut',
                ExtendedS3DestinationConfiguration={
                    'RoleARN': role_arn,
                    'BucketARN': f'arn:aws:s3:::{S3_BUCKET_NAME}',
                    'Prefix': 'YYYY/MM/DD',
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
        f'cd {lambda_dir} && zip -g lambda.zip lambda_runner.py'
    ]

    for cmd in commands:
        os.system(cmd)

    with open(f'{lambda_dir}/lambda.zip', 'rb') as f:
        zipped_code = f.read()

    return zipped_code


def create_lambda_function():
    client = boto3.client('lambda')
    response = client.create_function(
        FunctionName='load-tweets-to-es',
        Runtime='python3.6',
        Role='string',
        Handler='lambda_runner.lambda_handler',
        Code={
            'ZipFile': b'bytes',
            'S3Bucket': 'string',
            'S3Key': 'string',
            'S3ObjectVersion': 'string'
        },
        Description='Lambda function to process a tweet and push to ES',
        Timeout=900,
        MemorySize=1280
    )


if __name__ == '__main__':
    # create_s3_bucket()
    # create_kinesis_delivery_stream()
    package_lambda()
