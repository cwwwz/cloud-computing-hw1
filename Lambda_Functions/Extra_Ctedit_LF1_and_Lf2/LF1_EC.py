import json
import boto3
import datetime
import time
import os
import dateutil.parser
import logging

# Initialize the SQS client
sqs = boto3.client('sqs')

#EXTRA CREDIT
dynamodb = boto3.resource('dynamodb')
user_table = dynamodb.Table('state-previous-user-searches')

# SQS Queue URL 
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/851725404462/DiningConciergeQueue'
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

cuisines = ['indian', 'mexican', 'italian','thai','chinese','american','ethiopian', 'french','mediterranean']

def lambda_handler(event, context):
    print(event)
    intent_name = event['sessionState']['intent']['name']
    user_id = event['sessionId']
    
    # Check which intent is being processed
    if intent_name == 'GreetingIntent':
        print('greeting')
        return handle_greeting_intent(user_id)

    elif intent_name == 'ThankYouIntent':
        print('thank you')
        return handle_thank_you_intent()
    
    elif intent_name == 'DiningSuggestionsIntent':
        print('dining suggestion')
        return handle_dining_suggestion_intent(event, user_id)
    
    else:
        return fallback_response()


# Handle the Greeting Intent
def handle_greeting_intent(user_id):
    user_state = user_table.get_item(Key={'userid': user_id})
    print(user_state)
   
    if 'Item' in user_state:
        message = 'Welcome back! How can I help you today?'
    else:
        message = 'Hi there, how can I help you today?'
    
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
                'content': message
                
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
    
#EXTRA CREDIT
def save_user_state(user_id, location, cuisine, email):
    user_table.put_item(Item={
        'userid': user_id,
        'last_location': location,
        'last_cuisine': cuisine,
        'email': email
    })

    
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
def handle_dining_suggestion_intent(event, user_id):
    slots = event['sessionState']['intent']['slots']
    intent = event['sessionState']['intent']['name']
    user_state = user_table.get_item(Key={'userid': user_id})
    print('CONFIRMATION STATE:', event['sessionState']['intent']['confirmationState'])
    print(f"User state: {user_state}")
    

    if event['invocationSource'] == 'DialogCodeHook':
        if 'Item' in user_state and event['sessionState']['intent']['confirmationState'] == 'None' and slots['Location'] is None:
            # If user has previous preferences, ask if they want to reuse them
            return {
                'sessionState': {
                    'dialogAction': {
                        'type': 'ConfirmIntent'
                    },
                    'intent': {
                        'name': 'DiningSuggestionsIntent',
                        'slots': slots
                    }
                },
                'messages': [
                    {
                        'contentType': 'PlainText',
                        'content': 'I have your previous preferences. Would you like me to use the same preferences?'
                    }
                ]
            }
        # Step 2: Dialog Hook - Handle User's Response (Yes or No) to Reusing Preferences
        elif event['sessionState']['intent']['confirmationState'] == 'Confirmed':
            previous_request = user_state['Item']
            location = previous_request['last_location']
            cuisine = previous_request['last_cuisine']
            email = previous_request['email']
            
            # Send data to SQS without eliciting any new slots
            send_message_to_sqs(location, cuisine, -1, None, None, email)

            return {
                'sessionState': {
                    'dialogAction': {
                        'type': 'Close'
                    },
                    'intent': {
                        'name': 'DiningSuggestionsIntent',
                        'state': 'Fulfilled'
                    }
                },
                'messages': [
                    {
                        'contentType': 'PlainText',
                        'content': 'You will receive suggestions based on your previous preferences.'
                    }
                ]
            }

        # Step 3: If user denies or no previous state, continue with slot elicitation normally
        elif event['sessionState']['intent']['confirmationState'] == 'Denied' or 'Item' not in user_state or slots['Location']:
            # Now, we proceed to slot elicitation only after confirmation has been handled
            order_validation_result = validate_request(slots)
            
            # Step 4: Slot validation and elicitation happens here, only if necessary
            if not order_validation_result['isValid']:
                if 'message' in order_validation_result:
                    return {
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
                    return {
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
            else:
                return {
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
    elif event['invocationSource'] == 'FulfillmentCodeHook': 
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
        
        #EXTRA CREDIT
        try:
            user_id = event['sessionId']
            save_user_state(user_id, location, cuisine, email)
        except:
            print('ERROR: could not save user state')
        
        return {
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
