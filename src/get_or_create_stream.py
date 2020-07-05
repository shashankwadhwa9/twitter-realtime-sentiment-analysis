import boto3
import botocore
from local_settings import KINESIS_STREAM_NAME


def get_or_create_data_stream(client):
    """
    Gets or creates a kinesis data stream

    :param client: Kinesis client
    """
    try:
        # Check if the stream already exists
        response = client.describe_stream(StreamName=KINESIS_STREAM_NAME)
        print('Stream already exists')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # Create a new stream if it doesn't exist
            print('Stream does not exist, creating one now')
            response = client.create_stream(StreamName=KINESIS_STREAM_NAME, ShardCount=1)
        else:
            raise e

    return response


if __name__ == '__main__':
    kinesis_client = boto3.client('kinesis')
    res = get_or_create_data_stream(kinesis_client)
    print(res)
