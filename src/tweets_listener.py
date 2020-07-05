import argparse
import json
import boto3
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Variables that contain the user credentials to access Twitter API
from local_settings import KINESIS_STREAM_NAME, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET


class TweetStreamListener(StreamListener):
    # on success
    def on_data(self, data):
        print('On data')
        # decode json
        tweet = json.loads(data)
        if 'text' in tweet.keys():
            payload = {
                'id': str(tweet['id']),
                'tweet': str(tweet['text'].encode('utf8', 'replace')),
                'ts': str(tweet['created_at']),
            }

            try:
                kinesis_client.put_record(
                    StreamName=KINESIS_STREAM_NAME,
                    Data=json.dumps(payload),
                    PartitionKey='0'  # since we have only 1 shard
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
    parser.add_argument('--keywords', help='Keywords to search twitter for', required=True)
    args = parser.parse_args()

    # Create kinesis client connection
    kinesis_client = boto3.client('kinesis')

    # Create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    # Set twitter keys/tokens
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    # Create instance of the tweepy stream
    stream = Stream(auth, listener)

    # search twitter for tags or keywords passed, filter only India tweets
    stream.filter(track=args.keywords, languages=['en'])
    stream.filter(locations=[68.1766451354, 7.96553477623, 97.4025614766, 35.4940095078])
