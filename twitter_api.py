import os

import grequests
import requests
from requests_oauthlib import OAuth1

# These settings are taken from Environment variables
CONSUMER_KEY = os.environ.get('TWITTER_CONSUMER_KEY')
CONSUMER_SECRET = os.environ.get('TWITTER_CONSUMER_SECRET')
ACCESS_TOKEN = os.environ.get('TWITTER_ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')
API_URL = 'https://api.twitter.com/1.1/search/tweets.json'


def get_oauth1_authentication():
    """
    Set up the OAuth1 authentication with the Twitter API using the
    consumer key, consumer secret, access token, and access token secret.

    Returns:
        OAuth1: An instance of the OAuth1 object with the necessary credentials.
    """
    oauth = OAuth1(client_key=CONSUMER_KEY,
                   client_secret=CONSUMER_SECRET,
                   resource_owner_key=ACCESS_TOKEN,
                   resource_owner_secret=ACCESS_TOKEN_SECRET)
    return oauth


def get_number_of_tweets(query):
    """
    Get the number of tweets containing the given query using the Twitter API.

    Args:
        query (str, optional): The search query for tweets. Defaults to "California Gold Nutrition".

    Returns:
        int: The number of tweets containing the search query.

    Raises:
        ValueError: If there's an error in the API response.
    """
    params = {"q": query, "count": 150, "result_type": "recent"}
    # Send the request to the search endpoint
    response = requests.get(API_URL, params=params, auth=get_oauth1_authentication())

    # Check for errors in the response
    if response.status_code != 200:
        raise ValueError("Failed to get tweets (HTTP status code {})".format(response.status_code))

    # If no errors get the response and count the number of tweets containing the query
    json_response = response.json()
    return len(json_response["statuses"])



def get_number_of_tweets_async(query):
    """
    Get the number of tweets containing the given query using the Twitter API asynchronously.

    Args:
        query (str, optional): The search query for tweets. Defaults to "California Gold Nutrition".

    Returns:
        int: The number of tweets containing the search query.

    Raises:
        ValueError: If there's an error in the API response.
    """
    latitude = 32.109333
    longitude = 34.855499
    radius = "150km"

    params = {
        "q": query,
        "count": 150,
        "result_type": "recent",
        "geocode": f"{latitude},{longitude},{radius}"
    }

    # Create the grequests request
    request = grequests.get(API_URL, params=params, auth=get_oauth1_authentication())

    # Send the request asynchronously
    response = grequests.map([request])[0]

    # Check for errors in the response
    if response.status_code != 200:
        raise ValueError("Failed to get tweets (HTTP status code {})".format(response.status_code))

    # If no errors, get the response and count the number of tweets containing the query
    json_response = response.json()
    return len(json_response["statuses"])


if __name__ == "__main__":
    print(get_number_of_tweets(query="California Gold Nutrition"))
