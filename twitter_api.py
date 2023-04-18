import os

import requests
from requests_oauthlib import OAuth1

# These settings are taken from Environment variables
CONSUMER_KEY = os.environ.get('TWITTER_CONSUMER_KEY')
CONSUMER_SECRET = os.environ.get('TWITTER_CONSUMER_SECRET')
ACCESS_TOKEN = os.environ.get('TWITTER_ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')
API_URL = 'https://api.twitter.com/1.1/search/tweets.json'

def get_oauth1_authentication():
    # Set up the OAuth1 authentication
    oauth = OAuth1(client_key=CONSUMER_KEY,
                   client_secret=CONSUMER_SECRET,
                   resource_owner_key=ACCESS_TOKEN,
                   resource_owner_secret=ACCESS_TOKEN_SECRET)
    return oauth


def get_number_of_tweets(query = "California Gold Nutrition"):
    params = {"q": query, "count": 150, "result_type": "recent"}
    # Send the request to the search endpoint
    response = requests.get(API_URL, params=params, auth=get_oauth1_authentication())

    # Check for errors in the response
    if response.status_code != 200:
        raise ValueError("Failed to get tweets (HTTP status code {})".format(response.status_code))

    # If no errors get the response and count the number of tweets containing the query
    json_response = response.json()
    return len(json_response["statuses"])

if __name__ == "__main__":
    print(get_number_of_tweets(query = "California Gold Nutrition"))