import json
import boto3
import logging
import random
import urllib3
from base64 import b64encode

sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

dynamo_table = dynamodb.Table('yelp-restaurants')
opensearch_endpoint = 'https://search-cloud-computing-assignment1-qzalhaveyvfvrhow2pnugniohe.aos.us-east-1.on.aws' 

opensearch_username = 'admin'
opensearch_password = 'Cloudcomputing2024!!'

http = urllib3.PoolManager()

def lambda_handler(event, context):
    logger.debug(f"Received event: {json.dumps(event)}")
    for record in event['Records']:
        logger.debug(f"Message body: {record['body']}")
        
    for record in event['Records']:
        message = json.loads(record['body'])
        cuisine = message.get('cuisine')
        email = message.get('email')

        if not cuisine or not email:
            logger.debug("Missing required information (cuisine or email)")
            return

        try:
            restaurant_ids = get_restaurant_ids_from_es(cuisine)
        except Exception as e:
            logger.debug(f"Error querying OpenSearch: {str(e)}")
            return

        if not restaurant_ids:
            logger.debug(f"No restaurants found for cuisine: {cuisine}")
            return

        try:
            restaurant_details = get_restaurant_details_from_dynamodb(restaurant_ids)
        except Exception as e:
            logger.debug(f"Error querying DynamoDB: {str(e)}")
            return

        send_email(email, restaurant_details, message)

        logger.debug(f"Suggestions sent to {email}.")

def get_restaurant_ids_from_es(cuisine):
    es_query = f"{opensearch_endpoint}/_search?q=Cuisine:{cuisine}"
        
    auth_header = b64encode(f"{opensearch_username}:{opensearch_password}".encode()).decode('utf-8')
    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/json'
    }

    try:
        es_response = http.request('GET', es_query, headers=headers, timeout=urllib3.Timeout(10.0))
        if es_response.status != 200:
            raise Exception(f"Error: Received status code {es_response.status} from OpenSearch")
        
        data = json.loads(es_response.data.decode('utf-8'))
    except Exception as e:
        raise Exception(f"Failed to query OpenSearch: {str(e)}")

    try:
        es_data = data["hits"]["hits"]
    except KeyError:
        logger.debug("Error extracting hits from OpenSearch response")
        return []

    # Extract RestaurantIDs from OpenSearch results
    restaurant_ids = [restaurant["_source"]["RestaurantID"] for restaurant in es_data]

    # Return a random sample of 5 restaurant IDs
    return random.sample(restaurant_ids, min(5, len(restaurant_ids)))

def get_restaurant_details_from_dynamodb(restaurant_ids):
    details = []
    for restaurant_id in restaurant_ids:
        try:
            response = dynamo_table.get_item(Key={'BusinessID': restaurant_id})
            if 'Item' in response:
                details.append(response['Item'])
            else:
                logger.debug(f"Restaurant with ID {restaurant_id} not found in DynamoDB.")
        except Exception as e:
            logger.debug(f"Error fetching restaurant {restaurant_id}: {str(e)}")
    return details

def send_email(email, restaurant_details, user_request):
    cuisine = user_request.get('cuisine', 'Cuisine Not Provided')
    people = user_request.get('people', 'People Not Provided')
    dining_date = user_request.get('date', 'Date Not Provided')
    dining_time = user_request.get('time', 'Time Not Provided')

    subject = f"Your {cuisine} Restaurant Suggestions"
    
    body = (
        f"Hello! \n \n Here are my {cuisine} restaurant suggestions for {people} people, "
        f"for {dining_date} at {dining_time}:\n\n"
    )
    
    for i, restaurant in enumerate(restaurant_details, 1):
        body += f"{i}. {restaurant['Name']}, located at {restaurant['Address']}\n"
    
    body += "\n"
    body += "Bon Appetit!\n"
    body += "Your Dining Concierge"

    try:
        ses.send_email(
            Source='wc1609@nyu.edu',  # Ensure this is verified in SES
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        logger.debug(f"Email sent to {email}")
    except Exception as e:
        logger.debug(f"Error sending email: {str(e)}")

        print(f"Error sending email: {str(e)}")

   

