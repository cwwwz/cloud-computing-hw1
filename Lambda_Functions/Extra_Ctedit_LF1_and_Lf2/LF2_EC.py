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
sqs_url = 'https://sqs.us-east-1.amazonaws.com/851725404462/DiningConciergeQueue'

http = urllib3.PoolManager()

def lambda_handler(event, context):
    # Poll SQS queue for messages
    response = sqs.receive_message(
        QueueUrl=sqs_url,
    )

    # Check if there are messages
    if 'Messages' not in response:
        logger.info("No messages found in the queue. Exiting function.")
        return {
            'statusCode': 200,
            'body': "No messages to process."
        }

    # Process each message
    for message in response['Messages']:
        logger.debug(f"Message body: {message['Body']}")

        try:
            message_body = json.loads(message['Body'])
            cuisine = message_body.get('cuisine')
            email = message_body.get('email')

            if not cuisine or not email:
                logger.debug("Missing required information (cuisine or email).")
                continue

            # Query OpenSearch
            try:
                restaurant_ids = get_restaurant_ids_from_es(cuisine)
            except Exception as e:
                logger.debug(f"Error querying OpenSearch: {str(e)}")
                continue

            if not restaurant_ids:
                logger.debug(f"No restaurants found for cuisine: {cuisine}")
                continue

            # Fetch details from DynamoDB
            try:
                restaurant_details = get_restaurant_details_from_dynamodb(restaurant_ids)
            except Exception as e:
                logger.debug(f"Error querying DynamoDB: {str(e)}")
                continue

            # Send email
            send_email(email, restaurant_details, message_body)

            logger.debug(f"Suggestions sent to {email}.")

            # Delete processed message from SQS
            sqs.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=message['ReceiptHandle']
            )
            logger.debug(f"Deleted message from queue: {message['MessageId']}")

        except json.JSONDecodeError as e:
            logger.debug(f"Invalid JSON format: {str(e)}. Skipping this record.")
            continue

def get_restaurant_ids_from_es(cuisine):
    es_query = {
        "query": {
            "match": {
                "Cuisine": {
                    "query": cuisine,
                    "operator": "or"
                }
            }
        }
    }

    auth_header = b64encode(f"{opensearch_username}:{opensearch_password}".encode()).decode('utf-8')
    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/json'
    }

    es_response = http.request(
        'POST',
        f"{opensearch_endpoint}/_search",
        body=json.dumps(es_query),
        headers=headers
    )

    if es_response.status != 200:
        raise Exception(f"Error: Received status code {es_response.status} from OpenSearch")

    data = json.loads(es_response.data.decode('utf-8'))
    es_data = data["hits"]["hits"]

    restaurant_ids = [restaurant["_source"]["RestaurantID"] for restaurant in es_data]

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
    location = user_request.get('location', 'Location Not Provided')
    cuisine = user_request.get('cuisine', 'Cuisine Not Provided')
    people = user_request.get('number_of_people', 'People Not Provided')
    dining_date = user_request.get('date', 'Date Not Provided')
    dining_time = user_request.get('time', 'Time Not Provided')

    subject = f"Your {cuisine} Restaurant Suggestions"
    print('PEOPLE:', people)
    
    if int(people) > -1:
        body = (
            f"Hello! \n \n Here are my {cuisine} restaurant suggestions for {people} people, "
            f"for {dining_date} at {dining_time} in {location}:\n\n"
        )
    else:
        body = (
            f"Hello! \n \n Here are my suggestions for {cuisine} restaurants "
            f"in {location} based on your previous search:\n\n"
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

   

