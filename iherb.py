import argparse
import json
import logging

import pymysql
from fake_useragent import UserAgent
from pymysql.cursors import DictCursor

import requestiherb
import sql
import twitter_api

# Import all the Global variables from the configuration file
with open('conf.json', 'r') as f:
    config = json.load(f)

PARSER_TYPE = config["PARSER_TYPE"]
UA = UserAgent(browsers=config['BROWSERS'])
URL = config["URL"]
RESULTS_PER_PAGE = config["RESULTS_PER_PAGE"]
LOGGING_LEVEL = config["LOGGING_LEVEL"]
logging.basicConfig(filename=config["LOG_FILENAME"], level=logging.getLevelName(LOGGING_LEVEL),
                    format=config["LOG_FORMAT"])
connection = pymysql.connect(
    host=config['DATABASE']['HOST'],
    user=config['DATABASE']['USER'],
    password=config['DATABASE']['PASSWORD'],
    db=config['DATABASE']['DB'],
    charset=config['DATABASE']['CHARSET'],
    cursorclass=pymysql.cursors.DictCursor
)
DB_NAME = config["DB_NAME"]
CATEGORIES = config["CATEGORIES"]
DEFAULT_LIMIT = config["DEFAULT_LIMIT"]


def get_parameters_for_crapping():
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
    parser.add_argument('-c', '--category', type=str, metavar='', required=True, choices=config["CATEGORIES"],
                        help=f'Choose category from the following list:\n\n{config["CATEGORIES"]}')
    parser.add_argument('-l', '--limit', type=int, metavar='', help="(optional) number of results")
    arguments = parser.parse_args()
    if arguments.limit is None:
        lim = DEFAULT_LIMIT
    else:
        lim = arguments.limit
    return arguments, lim


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
    with connection.cursor() as cursor:
        cursor.execute(f'USE {DB_NAME}')
        sql.insert_categories_into_db(req.products, cursor)
        sql.insert_brands_into_db(req.products, cursor)
        sql.insert_inventory_status_into_db(req.products, cursor)
        sql.insert_product_into_db(req.products, cursor)
        brands = sql.get_brands_names(req.products, cursor)
        for index, brand in enumerate(brands):
            try:
                brands[index]['number_of_tweets'] = twitter_api.get_number_of_tweets_async(query=brands[index]["name"])
                print("Getting tweets request number ", index, " out of ", len(brands) - 1)
            except ValueError as err:
                logging.info(err)
                print("We reached the maximum number of requests")
                break
        sql.update_number_tweets(brands, cursor)
        connection.commit()


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
    args, limit = get_parameters_for_crapping()

    # Create an object Request_iherb
    req = requestiherb.RequestIherb(URL + args.category, limit)
    print(f"This request contains {min(limit, len(req.url_list))} pages of products")

    # Get the html of all the pages obtained with the request
    all_pages = requestiherb.get_html(req.url_list, limit)

    # Create the list of products from HTML pages
    create_product_list(all_pages)

    # Run the requests to populate and update the DB
    run_requests_on_db_and_api()
    print("THE END")
