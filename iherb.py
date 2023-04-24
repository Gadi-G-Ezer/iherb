import argparse
import json
import logging
import time
from fake_useragent import UserAgent

import requestiherb
import sql
import twitter_api

# Import all the Global variables from the configuration file
with open('conf.json', 'r') as f:
    config = json.load(f)

BROWSERS = config['BROWSERS']
LOG_FILENAME = config['LOG_FILENAME']
LOGGING_LEVEL = config['LOGGING_LEVEL']
LOG_FORMAT = config['LOG_FORMAT']
CATEGORIES = config['CATEGORIES']
DEFAULT_LIMIT = config['DEFAULT_LIMIT']
TIME_SLEEP = config['TIME_SLEEP']
URL = config['URL']


UA = UserAgent(browsers=BROWSERS)
logging.basicConfig(filename=LOG_FILENAME, level=logging.getLevelName(LOGGING_LEVEL),
                    format=LOG_FORMAT)


def get_parameters_for_scrapping():
    """
    Retrieve parameters for running a web scraping script.

    Args:
        None.

    Returns:
        A tuple containing two elements:
        - A Namespace object containing the command-line parameters retrieved using argparse.
        - The limit of results to retrieve, which can be either the value specified by the user or the default value.

    Raises:
        No exception is raised.

    Description:
        This function uses argparse to retrieve the command-line parameters required for running a web scraping script.
        The required parameters are:
        - The category for which to retrieve the scraping results, which must be chosen from a predefined list of
        categories.
        - (Optional) The maximum number of results to retrieve, which must be a positive integer.

        If the user does not specify the limit parameter, the default limit will be used.

        The function returns a tuple containing two elements:
        - A Namespace object containing the command-line parameters retrieved using argparse.
        - The limit of results to retrieve, which can be either the value specified by the user or the default value.

        This function does not raise any exception.
    """
    parser = argparse.ArgumentParser(description='Take a query')
    parser.add_argument('-c', '--category', type=str, metavar='', required=True, choices=CATEGORIES,
                        help=f'Choose category from the following list:\n\n{CATEGORIES}')
    parser.add_argument('-l', '--limit', type=int, metavar='', help="(optional) number of results")
    arguments = parser.parse_args()
    if arguments.limit is None:
        lim = DEFAULT_LIMIT
    else:
        lim = arguments.limit
    return arguments, lim


def pause_program(pause_time=TIME_SLEEP):
    """
    Pauses the program for a specified duration.

    This function is useful for waiting before sending more requests to an API,
    such as the Twitter API, to avoid hitting rate limits. It prints a message
    before and after the pause.

    Args:
        pause_time (int, optional): The duration of the pause in seconds. Default value is TIME_SLEEP.

    Returns:
        None
    """
    print(f"We reached the maximum number of requests.")
    print(f"Pausing for {TIME_SLEEP} seconds before to continue the requests on Twitter API...")
    time.sleep(pause_time)
    print("Done pausing!")


def run_requests_on_db_and_api():
    """
    This function inserts product data into the database and retrieves the number of tweets
    related to each brand using the Twitter API.

    It performs the following operations:
        1. Sets the database to use.
        2. Inserts categories, brands, inventory statuses, and products into the database.
        3. Retrieves brand names from the database.
        4. Fetches the number of tweets associated with each brand name using the Twitter API.
        5. Updates the number of tweets in the database for each brand.
        6. Commits the changes to the database.

    Note: The function assumes that the 'connection', 'sql', and 'twitter_api' objects
    have been properly initialized outside the function.
    """
    sql.insert_categories_into_db(req.products)
    sql.insert_brands_into_db(req.products)
    sql.insert_inventory_status_into_db(req.products)
    sql.insert_product_into_db(req.products)
    brands = sql.get_brands_names(req.products)
    for index, brand in enumerate(brands):
        continue_requests = True
        while continue_requests:
            try:
                brands[index]['number_of_tweets'] = twitter_api.get_number_of_tweets_async(query=brands[index]["name"])
                print("Getting tweets request number ", index, " out of ", len(brands) - 1)
                continue_requests = False
            except ValueError as err:
                logging.info(err)
                pause_program(pause_time=TIME_SLEEP)
        sql.update_number_tweets(brands)


def create_product_list(page_list):
    """
    Create a list of products from the provided list of pages using the global 'req' object.

    This function iterates through the pages and retrieves detailed product information
    for each page using the 'req' object (an instance of the iHerbRequest class).
    It also prints the progress of processing each page and the total number of products scraped.

    Args:
        page_list (list): A list of pages to extract product information from.

    Note: The function assumes that the 'get_detailed_information_from_html' method is
    available in the 'req' object (iHerbRequest instance) and updates the 'products' attribute of the object.
    """
    for i, page in enumerate(page_list):
        print(f"process the page : {i}")
        req.get_detailed_information_from_html(page)
        print(f"success processing the page : {i}")

    print(f"Total number of product scrapped = {len(req.products)}")


if __name__ == '__main__':
    args, limit = get_parameters_for_scrapping()

    # Create an object of RequestIherb
    req = requestiherb.RequestIherb(URL + args.category, limit)
    print(f"This request contains {min(limit, len(req.url_list))} pages of products")

    # Get the html of all the pages obtained with the request
    all_pages = requestiherb.get_html(req.url_list, limit)

    # Create the list of products from HTML pages
    create_product_list(all_pages)

    # Run the requests to populate and update the DB
    run_requests_on_db_and_api()
    print("THE END")
