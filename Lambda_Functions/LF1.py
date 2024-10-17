import json
import boto3
import datetime
import time
import os
import dateutil.parser
import logging

# Initialize the SQS client
sqs = boto3.client('sqs')

# SQS Queue URL 
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/851725404462/DiningConciergeQueue'
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

cuisines = ['indian', 'mexican', 'ttalian','thai','chinese','american','ethiopian', 'french','mediterranean']

def lambda_handler(event, context):
    print(event)
    intent_name = event['sessionState']['intent']['name']
    
    # Check which intent is being processed
    if intent_name == 'GreetingIntent':
        print('greeting')
        return handle_greeting_intent()

    elif intent_name == 'ThankYouIntent':
        print('thank you')
        return handle_thank_you_intent()
    
    elif intent_name == 'DiningSuggestionsIntent':
        print('dining suggestion')
        return handle_dining_suggestion_intent(event)
    
    else:
        return fallback_response()


# Handle the Greeting Intent
def handle_greeting_intent():
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'GreetingIntent',
                'state': 'Fulfilled'
            }
        },
        'messages': [
            {
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help you today?'
                
            }
        ]
    }


# Handle the Thank You Intent
def handle_thank_you_intent():
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'ThankYouIntent',
                'state': 'Fulfilled'
            }
        },
        # 'messages': [
        #     {
        #         'contentType': 'PlainText',
        #         'content': 'You’re welcome. Have a great day!'
        #     }
        # ]
    }
    
#check if given date is today or in the future    
def isvalid_date(user_date):
    try:
        given_date = dateutil.parser.parse(user_date).date()
        today = datetime.date.today()
        return given_date >= today
    except ValueError:
        return False
    
#check if number of people is a positive integer    
def isvalid_num(num):
    if num is not None and int(num) > 0 and float(num).is_integer():
        return True
    return False
    
def isvalid_city(city):
    valid_cities = ['new york', 'manhattan', 'new york city', 'ny', 'nyc']
    return city.lower() in valid_cities
    
    
#check if time is valid and in the     
def isvalid_time(time, date):
    try: 
        given_time = datetime.datetime.strptime(time, '%H:%M').time()
        given_date = dateutil.parser.parse(date).date()
        today = datetime.date.today()
        if given_date == today:
            return given_time >= datetime.datetime.now().time()
        return True
    except ValueError:
        return False
            
def validate_request(slots):
    if not slots['Location']:
        logger.debug('validating location')
        return {
            'isValid': False,
            'invalidSlot': 'Location'
        }
        
    logger.debug(slots['Location']['value']['interpretedValue'].lower())    
    if not isvalid_city(slots['Location']['value']['interpretedValue'].lower()):
        logger.debug('invalid city')
        return {
            'isValid': False,
            'invalidSlot': 'Location',
            'message': "Please try another city."
        }
    
    if not slots['Cuisine']:
        logger.debug('validating cuisine')
        return {
            'isValid': False,
            'invalidSlot': 'Cuisine'
        }
        
    if slots['Cuisine']['value']['interpretedValue'].lower() not in cuisines:
        logger.debug('invalid cuisine')
        return {
            'isValid': False,
            'invalidSlot': 'Cuisine',
            'message': 'Please select one of the following cuisines: {}.'.format(", ".join(cuisines))
        }
        
    if not slots['Number_of_People']:
        print('validating number_of_people')
        return {
            'isValid': False,
            'invalidSlot': 'Number_of_People'
        }
        
    if not isvalid_num(slots['Number_of_People']['value']['interpretedValue']):
        print('invalid number_of_people')
        return {
            'isValid': False,
            'invalidSlot': 'Number_of_People',
            'message': "Please enter a positive integer for number of people"
        }
        
    if not slots['Date']:
        print('validating date')
        return {
            'isValid': False,
            'invalidSlot': 'Date'
        }
        
    if not isvalid_date(slots['Date']['value']['interpretedValue']):
        print('invalid date')
        return {
            'isValid': False,
            'invalidSlot': 'Date',
            'message': "Please enter a present or future date"
        }
    
    if not slots['Time']:
        print('validating time')
        return {
            'isValid': False,
            'invalidSlot': 'Time'
        }
        
    if not isvalid_time(slots['Time']['value']['interpretedValue'], slots['Date']['value']['interpretedValue']):
        print('invalid time')
        return {
            'isValid': False,
            'invalidSlot': 'Time',
            'message': "Please enter a present or future time"
        }
    return {'isValid': True}


# Handle the Dining Suggestion Intent
def handle_dining_suggestion_intent(event):
    logger.debug(f"Received Lex event: {json.dumps(event)}")
    slots = event['sessionState']['intent']['slots']
    intent = event['sessionState']['intent']['name']
    order_validation_result = validate_request(slots)
    
    
    if event['invocationSource'] == 'DialogCodeHook':
        if not order_validation_result['isValid']:
            if 'message' in order_validation_result:
                response = {
                    "sessionState": {
                        "dialogAction": {
                            "slotToElicit": order_validation_result['invalidSlot'],
                            "type": "ElicitSlot"
                        },
                        "intent": {
                            "name": intent,
                            "slots": slots
                        }
                    },
                    "messages": [
                        {
                            "contentType": "PlainText",
                            "content": order_validation_result['message']
                        }
                    ]
                }
            else:
                response = {
                    "sessionState": {
                        "dialogAction": {
                            "slotToElicit": order_validation_result['invalidSlot'],
                            "type": "ElicitSlot"
                        },
                        "intent": {
                            "name": intent,
                            "slots": slots
                        }
                    }
                }
            #     response = {
            #     "sessionState": {
            #         "dialogAction": {
            #             "type": "Delegate"
            #         },
            #         "intent": {
            #             'name': intent,
            #             'slots': slots
            #         }
            #     }
            # }
        else:
            response = {
                "sessionState": {
                    "dialogAction": {
                        "type": "Delegate"
                    },
                    "intent": {
                        'name': intent,
                        'slots': slots
                    }
                }
            }
    
    # Once all slots are filled, send data to SQS
    #if location and cuisine and number_of_people and date and time and email:
    if event['invocationSource'] == 'FulfillmentCodeHook': 
        if event['invocationSource'] == 'FulfillmentCodeHook': 
            logger.debug("Entering FulfillmentCodeHook")
        location = slots['Location']['value']['interpretedValue']
        cuisine = slots['Cuisine']['value']['interpretedValue']
        number_of_people = slots['Number_of_People']['value']['interpretedValue']
        date = slots['Date']['value']['interpretedValue']
        time = slots['Time']['value']['interpretedValue']
        email = slots['Email']['value']['interpretedValue']
        logger.debug(f"Location: {location}, Cuisine: {cuisine}, People: {number_of_people}, Date: {date}, Time: {time}, Email: {email}")
        send_message_to_sqs(location, cuisine, number_of_people, date, time, email)
        print('FULFILLED')
        
        response = {
            'sessionState': {
                'dialogAction': {
                    'type': 'Close'
                },
                'intent': {
                    'name': 'DiningSuggestionsIntent',
                    "slots": slots,
                    'state': 'Fulfilled'
                }
            },
            'messages': [
                {
                    'contentType': 'PlainText',
                    'content': 'You’re all set. Expect my suggestions shortly! Have a good day.'
                }
            ]
        }
    return response


# Send the data to SQS
def send_message_to_sqs(location, cuisine, number_of_people, date, time, email):
    message_body = {
        'location': location,
        'cuisine': cuisine,
        'number_of_people': number_of_people,
        'date': date,
        'time': time,
        'email': email
    }

    try:
        logger.debug(f"Sending message to SQS: {message_body}")
        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message_body)
        )
        logger.debug(f"Message sent to SQS. Response: {response}")
    except Exception as e:
        logger.error(f"Failed to send message to SQS: {str(e)}")


# Continue the conversation by delegating control back to Lex
def delegate_to_continue(event):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Delegate'
            },
            'intent': event['sessionState']['intent']
        }
    }


# Fallback response in case of unexpected intent
def fallback_response():
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'FallbackIntent',
                'state': 'Failed'
            }
        },
        'messages': [
            {
                'contentType': 'PlainText',
                'content': 'I’m sorry, I didn’t understand that. Can you please repeat?'
            }
        ]
    }
