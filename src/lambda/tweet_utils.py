import re
from textblob import TextBlob


def get_sentiment(text):
    """
    Get sentiment of a tweet using TextBlob package
    """
    blob = TextBlob(text)
    sentiment_polarity = blob.sentiment.polarity
    if sentiment_polarity < 0:
        sentiment = 'negative'
    elif sentiment_polarity <= 0.2:
        sentiment = 'neutral'
    else:
        sentiment = 'positive'

    return sentiment


def process_tweet(tweet):
    """
    Create tweet dict to load to ES
    """
    tweet_dict = {
        'id': tweet['id_field'],
        'tweet': tweet['text'],
        'timestamp': tweet['timestamp_ms'],
        'user': {
            'id': tweet['user']['id'],
            'name': tweet['user']['name']
        },
        'sentiment': get_sentiment(tweet['text']),
        'coordinates': tweet['coordinates'],
        'hashtags': [x['text'] for x in tweet['entities']['hashtags']],
        'mentions': re.findall(r'@\w*', tweet['text'])
    }

    return tweet_dict
