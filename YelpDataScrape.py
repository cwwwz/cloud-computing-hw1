import requests
import boto3
from datetime import datetime
from decimal import Decimal

# Yelp API configuration
API_KEY = #ADD YOUR OWN API KEY HERE
headers = {
    'Authorization': f'Bearer {API_KEY}',
}

# DynamoDB configuration
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('yelp-restaurants')

# Function to search Yelp API for restaurants
def search_restaurants(term, location, limit, offset):
    url = 'https://api.yelp.com/v3/businesses/search'
    params = {
        'term': term,
        'location': location,  # Limit to Manhattan, NY
        'categories': 'restaurants',
        'limit': limit,
        'offset': offset
    }
    response = requests.get(url, headers=headers, params=params)

    print(f"API call at offset {offset} returned status code {response.status_code}")

    if response.status_code != 200:
        print(f"Error: {response.json()}")
    return response.json()

# Function to check if a restaurant exists in DynamoDB
def check_if_restaurant_exists(business_id):
    try:
        response = table.get_item(Key={'BusinessID': business_id})
        return 'Item' in response
    except Exception as e:
        print(f"Error checking restaurant with BusinessID {business_id}: {e}")
        return False

# Function to insert restaurant data into DynamoDB
def insert_restaurant(restaurant):
    try:
        if check_if_restaurant_exists(restaurant['id']):
            print(f"Restaurant with BusinessID {restaurant['id']} already exists. Skipping insertion.")
            return False

        cuisines = ', '.join([category['title'] for category in restaurant['categories']])
        latitude = Decimal(str(restaurant['coordinates']['latitude']))
        longitude = Decimal(str(restaurant['coordinates']['longitude']))
        rating = Decimal(str(restaurant['rating']))

        # Insert data into DynamoDB
        table.put_item(
            Item={
                'BusinessID': restaurant['id'],  # Primary Key
                'Name': restaurant['name'],
                'Address': ', '.join(restaurant['location']['display_address']),
                'Coordinates': {
                    'Latitude': latitude,
                    'Longitude': longitude
                },
                'NumberOfReviews': restaurant['review_count'],
                'Rating': rating,
                'ZipCode': restaurant['location'].get('zip_code', 'N/A'),
                'InsertedAtTimestamp': str(datetime.now()),
                'Cuisine': cuisines  # Insert all cuisines as a comma-separated string
            }
        )
        print(f"Inserted restaurant: {restaurant['name']} with cuisines: {cuisines}")
        return True
    except Exception as e:
        print(f"Failed to insert restaurant {restaurant['name']}: {e}")
        return False


def scrape_and_insert(term, neighborhoods, max_results):
    total_inserted = 0
    batch_size = 50  # Yelp API limit per call

    # Loop through each neighborhood
    for neighborhood in neighborhoods:
        location = f"{neighborhood}, Manhattan, NY"
        offset = 0
        total_retrieved = 0

        print(f"Starting search for neighborhood: {neighborhood}")

        while total_retrieved < max_results:
            remaining_results = min(batch_size, max_results - total_retrieved)

            # Fetch data for the current neighborhood
            data = search_restaurants(term, location, limit=remaining_results, offset=offset)

            total_available = data.get('total', 0)
            businesses = data.get('businesses', [])

            if not businesses:
                print(f"No more businesses found for {neighborhood}.")
                break

            for restaurant in businesses:
                if insert_restaurant(restaurant):
                    total_inserted += 1

            total_retrieved += len(businesses)
            offset += batch_size

            # Stop if we've reached the max results for this neighborhood
            if total_retrieved >= max_results:
                break
    print(f"Total number of restaurants successfully inserted: {total_inserted}")

neighborhoods = [
    'Upper East Side',
    'Harlem',
    'Midtown',
    'Chelsea',
    'East Village',
    'Tribeca',
    'West Village',
    'Financial District',
    'SoHo',
    'Lower East Side'
]

scrape_and_insert('Japanese', neighborhoods, 240)

