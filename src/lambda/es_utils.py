import boto3
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import bulk
from local_settings import ES_HOST, REGION

INDEX_NAME = 'tweets'
INDEX_MAPPING = {
    'properties': {
        'id': {
            'type': 'text'
        },
        'tweet': {
            'type': 'text'
        },
        'timestamp': {
            'type': 'date'
        },
        'user': {
            'properties': {
                'id': {
                    'type': 'long'
                },
                'name': {
                    'type': 'text'
                }
            }
        },
        'sentiment': {
            'type': 'keyword'
        },
        'coordinates': {
            'properties': {
                'coordinates': {
                    'type': 'geo_point'
                },
                'type': {
                    'type': 'keyword'
                }
            }
        },
        'hashtags': {
            'type': 'text'
        },
        'mentions': {
            'type': 'text'
        }
    }
}


def divide_into_chunks(l, n):
    """
    Divide a list l into chunks of size n
    """
    return [l[i:i+n] for i in range(0, len(l), n)]


def load_to_es(tweets):
    """
    Load tweets to ElasticSearch using the bulk insert API
    """
    # https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-request-signing.html#es-request-signing-python
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, service, session_token=credentials.token)
    es = Elasticsearch(
        hosts=[{'host': ES_HOST, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # Create index if not exists
    if not es.indices.exists(INDEX_NAME):
        print(f'Creating index {INDEX_NAME}')
        es.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)
    else:
        # Update mapping
        print('Updating mapping')
        es.indices.put_mapping(index=INDEX_NAME, body=INDEX_MAPPING)

    # Load the data to ES using bulk insert
    chunks = divide_into_chunks(tweets, 1000)
    total_chunks = len(chunks)
    for ctr, chunk in enumerate(chunks, 1):
        print(f'Loading data of chunk {ctr} of {total_chunks} to ES')
        es_docs = []
        # Convert data to ES bulk load format
        for doc in chunks:
            bulk_doc = {
                '_index': INDEX_NAME,
                '_id': doc['id'],
                '_source': doc
            }
            es_docs.append(bulk_doc)

        bulk(es, es_docs)
