import argparse
import json
import boto3
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Variables that contain the user credentials to access Twitter API
from local_settings import KINESIS_DELIVERY_STREAM, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET


class TweetStreamListener(StreamListener):
    # on success
    def on_data(self, data):
        print('On data')
        tweet = json.loads(data)
        try:
            if 'text' in tweet.keys():
                message = json.dumps(tweet)
                message = message + ',\n'
                kinesis_client.put_record(
                    DeliveryStreamName=KINESIS_DELIVERY_STREAM,
                    Record={
                        'Data': message
                    }
                )
        except Exception as e:
            print(e)

        return True

    # on failure
    def on_error(self, status):
        print('On error')
        print(status)


if __name__ == '__main__':
    # Accept keywords
    parser = argparse.ArgumentParser()
    parser.add_argument('--keyword', type=str, help='Keyword to search twitter for', required=True)
    args = parser.parse_args()

    # Create kinesis client connection
    kinesis_client = boto3.client('firehose')

    # Create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    # Set twitter keys/tokens
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    # Create instance of the tweepy stream
    stream = Stream(auth, listener)

    # search twitter for passed keyword
    stream.filter(track=[args.keyword], languages=['en'])
