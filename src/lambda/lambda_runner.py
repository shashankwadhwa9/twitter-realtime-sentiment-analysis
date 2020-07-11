import json
import boto3
from tweet_utils import process_tweet
from es_utils import load_to_es

s3 = boto3.client('s3')


def lambda_handler(event, context):
    print('Lambda execution started')
    print(f"Total record sets: {len(event['Records'])}")

    for record in event['Records']:
        # Get the bucket name and key for the new file
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Get object from s3
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
        except Exception as e:
            print(e)
            print(f'ERROR: Error getting key {key} from bucket {bucket}')
            continue

        # Parse the json
        try:
            s3_file_content = obj['Body'].read().decode('utf-8')

            # clean the content
            if s3_file_content.endswith(',\n'):
                s3_file_content = s3_file_content[:-2]
            tweets_str = '[' + s3_file_content + ']'
            tweets = json.loads(tweets_str)
        except Exception as e:
            print(e)
            print(f'ERROR: Error loading json from {key} in bucket {bucket}')
            continue

        # Transform the tweets, add sentiment data
        processed_tweets = [process_tweet(tweet) for tweet in tweets]
        print('Tweets processed')

        # Load tweets to ES
        try:
            load_to_es(processed_tweets)
        except Exception as e:
            print(e)
            print(f'ERROR: Error loading tweets data to ES for {key} of bucket {bucket}')
            continue

    print('Lambda execution completed')
