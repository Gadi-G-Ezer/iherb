import json
import os
import time

import grequests
from requests_oauthlib import OAuth1

# Import all the Global variables from the configuration file
with open('conf.json', 'r') as f:
    config = json.load(f)

# These settings are taken from Environment variables
CONSUMER_KEY = os.environ.get('TWITTER_CONSUMER_KEY')
CONSUMER_SECRET = os.environ.get('TWITTER_CONSUMER_SECRET')
ACCESS_TOKEN = os.environ.get('TWITTER_ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')
API_URL = 'https://api.twitter.com/1.1/search/tweets.json'
REQUEST_ACCEPTED_CODE = config["TWITTER_REQUEST_PARAMETERS"]["REQUEST_ACCEPTED_CODE"]


def get_oauth1_authentication():
    """
    Set up the OAuth1 authentication with the Twitter API using the
    consumer key, consumer secret, access token, and access token secret.

    Returns:
        OAuth1: An instance of the OAuth1 object with the necessary credentials.
    """
    oauth = OAuth1(client_key=CONSUMER_KEY,
                   client_secret=CONSUMER_SECRET)
    return oauth


def run_request(query, max_id):
    """
    Sends a request to the Twitter API with the specified query and max_id.

    This function creates and sends an asynchronous request to the Twitter API
    using the grequests library. It includes search parameters such as the query,
    count, result type, and geocode, and it applies the OAuth1 authentication for
    the API.

    Args:
        query (str): The search query for the Twitter API request.
        max_id (int or None): The maximum tweet ID to fetch in the request. Set to None if not used.

    Returns:
        response_: The response object from the grequests request.
    """
    params = {
        "q": query,
        "count": config["TWITTER_REQUEST_PARAMETERS"]["MAX_TWEETS"],
        "result_type": config["TWITTER_REQUEST_PARAMETERS"]["RESULT_TYPE"],
        "geocode": f'{config["TWITTER_REQUEST_PARAMETERS"]["LATITUDE"]},'
                   f'{config["TWITTER_REQUEST_PARAMETERS"]["LONGITUDE"]},'
                   f'{config["TWITTER_REQUEST_PARAMETERS"]["RADIUS"]}'
    }
    if max_id:
        params["max_id"] = max_id

    # Create the grequests request
    request = grequests.get(API_URL, params=params, auth=get_oauth1_authentication())

    # Send the request asynchronously
    response_ = grequests.map([request])[0]
    return response_


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
    tweet_count = 0
    max_id = None
    while True:
        # Send the request asynchronously
        response = run_request(query, max_id)

        # Check for errors in the response
        if response.status_code != REQUEST_ACCEPTED_CODE:
            raise ValueError("Failed to get tweets (HTTP status code {})".format(response.status_code))

        # If no errors, get the response and count the number of tweets containing the query
        json_response = response.json()
        tweet_count += len(json_response["statuses"])

        # Check if there are more results
        metadata = json_response.get("search_metadata")
        next_results = metadata.get("next_results") if metadata else None
        if not next_results:
            break
        max_id = int(next_results.split('max_id=')[1].split('&')[0]) - 1  # Extract the max_id from the next_results par
        time.sleep(1)  # Add a delay to avoid hitting rate limits

    return tweet_count


if __name__ == "__main__":
    print(get_number_of_tweets_async(query="California Gold Nutrition"))
