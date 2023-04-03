import json
import logging
import re

import grequests
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

import product

with open('conf.json', 'r') as f:
    config = json.load(f)

PARSER_TYPE = config["PARSER_TYPE"]
UA = UserAgent(browsers=config['BROWSERS'])
URL = config["URL"]
RESULTS_PER_PAGE = config["RESULTS_PER_PAGE"]
LOGGING_LEVEL = config["LOGGING_LEVEL"]
logging.basicConfig(filename=config["LOG_FILENAME"], level=logging.getLevelName(LOGGING_LEVEL),
                    format=config["LOG_FORMAT"])

DB_NAME = config["DB_NAME"]
CATEGORIES = config["CATEGORIES"]
CATEGORY = "sports"
DEFAULT_LIMIT = config["DEFAULT_LIMIT"]


class RequestIherb:
    """
    A class to represent a request to the iHerb website.

    Attributes
    ----------
    url : str
        The URL to request. Defaults to the URL for the Sports category on iHerb.
    nb_result : int
        The total number of search results.
    url_list : list of str
        The URLs for each page of search results.

    Methods
    -------
    _get_nb_results():
        Private method to get the total number of search results.
    _get_url_list():
        Private method to generate the URLs for each page of search results.
    get_detailed_information_from_html(html):
        Extracts detailed information about products from the HTML of a search results page.
    """

    def __init__(self, url=URL + CATEGORY, limit=DEFAULT_LIMIT):
        self.url = url
        self.nb_result = self._get_nb_results()
        self.url_list = self._get_url_list()
        self.products = []
        self.limit = limit

    def _get_nb_results(self):
        html = self._get_page_content()
        soup = BeautifulSoup(html, PARSER_TYPE)
        span_tag = soup.find('span', {'class': 'sub-header-title display-items'})
        text = span_tag.text
        # The total number of products of the category is stored in "text" between the char 21 and 25
        nb_result = int(text[21:25])
        return nb_result

    def _get_url_list(self):
        nb_pages = int(self.nb_result / RESULTS_PER_PAGE) + 2
        list_of_url = []
        for i in range(1, nb_pages):
            list_of_url.append(self.url + "?p=" + str(i))
        return list_of_url

    def get_detailed_information_from_html(self, html):
        soup = BeautifulSoup(html, PARSER_TYPE)
        for item in soup.find_all('div', class_=r'product-inner product-inner-wide'):
            url = item.a['href']
            name = item.findNext('div', attrs={'itemprop': "name", 'class': 'product-title'}).text
            try:
                rating = float(item.findNext('meta', {'itemprop': 'ratingValue'})['content'])
                nb_reviews = int(item.findNext('meta', {'itemprop': 'reviewCount'})['content'])
            except TypeError as e:
                rating = 0
                nb_reviews = 0
                print(f"No rating or review for the product {name}. Exception = {e}")
            product_id = int(item.a['data-product-id'])
            part_no = item.a['data-part-number']
            brand_name = item.a['data-ga-brand-name']
            brand_id = item.a['data-ga-brand-id']
            discount_price = float(re.sub(r'[^\d.]', '', item.a['data-ga-discount-price']))
            out_of_stock = item.a['data-ga-is-out-of-stock']
            has_discount = item.a['data-ga-is-discontinued']
            inventory_status = item.a['data-ga-inventory-status']
            currency = item.findNext('meta', attrs={'itemprop': 'priceCurrency'})['content']
            price = item.findNext('meta', attrs={'itemprop': 'price'})['content']
            category = item.findNext('div', attrs={'itemprop': 'category'})['content']
            self.products.append(
                product.Product(url=url, name=name, rating=rating, nb_reviews=nb_reviews, image=None,
                                product_id=product_id,
                                part_no=part_no, brand_name=brand_name, brand_id=brand_id,
                                discount_price=discount_price, out_of_stock=out_of_stock, has_discount=has_discount,
                                inventory_status=inventory_status, currency=currency, price=price, category=category))
        return None

    def _get_page_content(self):
        """
        Sends a GET request to the given URL and returns the content of the response.

        Returns
        -------
        bytes
            The content of the response.
        """
        headers = {"User-Agent": UA.random}
        response = requests.get(self.url, headers=headers)
        if response.ok:
            print(f'We could access the URL {self.url}')
        else:
            print(f'We could access the URL but we did not get the content of this URL {self.url}')
        html = response.content
        return html


def get_html(urls, limit):
    """
    Sends GET requests to the given URLs using grequests and returns the content of the responses.

    Parameters
    ----------
    urls : list of str
        The URLs to request.
    limit: maximum number of pages to parse

    Returns
    -------
    list of str
        The content of the responses, in the same order as the input URLs.
    """
    all_requests = [grequests.get(url) for url in urls]
    # Send the requests and process the responses
    responses = []
    for index, request in enumerate(all_requests):
        if index < limit:
            response = request.send()
            responses.append(response)
            print(f"Got response from {response.url}")
            logging.info(f"Got response from {response.url}")
        else:
            break

    # Extract the content of each response and print it
    contents = []
    for obj in responses:
        content = obj.response.content.decode('utf-8')
        contents.append(content)
        print(f"Extracted content from {obj.url}")
        logging.info(f"Extracted content from {obj.url}")
    return contents
