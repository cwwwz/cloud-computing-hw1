import boto3
import json
from decimal import Decimal
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'search-cloud-computing-assignment1-qzalhaveyvfvrhow2pnugniohe.aos.us-east-1.on.aws'
client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

dynamodb = boto3.resource('dynamodb', region_name=region)
table = dynamodb.Table('yelp-restaurants')


def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def push_dynamodb_to_opensearch():
    last_evaluated_key = None
    total_items_indexed = 0
    total_items_skipped = 0

    while True:
        if last_evaluated_key:
            response = table.scan(
                ExclusiveStartKey=last_evaluated_key,
                Limit=100
            )
        else:
            response = table.scan(Limit=100)

        items = response['Items']

        for item in items:
            restaurant_id = item.get('BusinessID')
            cuisine = item.get('Cuisine')

            if client.exists(index='restaurants', id=restaurant_id):
                print(f"Document {restaurant_id} already exists. Skipping.")
                total_items_skipped += 1
                continue

            document = {
                'RestaurantID': restaurant_id,
                'Cuisine': cuisine
            }

            try:
                es_response = client.index(index='restaurants', id=restaurant_id, body=document)
                total_items_indexed += 1
                print(f"Indexed RestaurantID {restaurant_id} with Cuisine {cuisine}: {es_response['result']}")
            except Exception as e:
                print(f"Failed to index document {restaurant_id}: {str(e)}")

        last_evaluated_key = response.get('LastEvaluatedKey')
        print(f"LastEvaluatedKey: {last_evaluated_key}")

        if not last_evaluated_key:
            print("No more items to scan, exiting.")
            break

    print(f"Total items indexed: {total_items_indexed}")
    print(f"Total items skipped (already exist): {total_items_skipped}")
    total_documents_in_opensearch = client.count(index="restaurants")['count']
    print(f"Total number of documents in OpenSearch: {total_documents_in_opensearch}")


push_dynamodb_to_opensearch()


