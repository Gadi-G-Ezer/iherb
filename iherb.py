import re

import grequests
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

UA = UserAgent(browsers=['edge', 'chrome', 'firefox'])
PARSER_TYPE = "html.parser"
URL = "https://www.iherb.com/c/sports"
RESULTS_PER_PAGE = 48


class Generator:
    def __init__(self, obj):
        self.obj = obj


class Request_iherb:
    def __init__(self, url=URL):
        self.url = url
        self.nb_result = self._get_nb_results()
        self.url_list = self._get_url_list()
        # self.product_list = []

    def _get_nb_results(self):
        html = get_page_content(self.url)
        soup = BeautifulSoup(html, PARSER_TYPE)
        span_tag = soup.find('span', {'class': 'sub-header-title display-items'})
        text = span_tag.text
        nb_result = int(text[21:25])
        return nb_result

    def _get_url_list(self):
        nb_pages = int(self.nb_result / RESULTS_PER_PAGE) + 2
        list_of_url = []
        for i in range(1, nb_pages):
            list_of_url.append(self.url + "?p=" + str(i))
        return list_of_url

    def get_detailed_information_from_html(self, html):
        global link, product, price, rating_review, rating, review
        soup = BeautifulSoup(html, PARSER_TYPE)
        products = []
        for item in soup.find_all('div', class_=r'product-inner product-inner-wide'):
            url = item.a['href']
            name = item.findNext('div', attrs={'itemprop': "name", 'class': 'product-title'}).text
            rating = float(item.findNext('meta', {'itemprop': 'ratingValue'})['content'])
            nb_reviews = int(item.findNext('meta', {'itemprop': 'reviewCount'})['content'])
            # image_url = item.findNext('span', {'class': 'product-image'}).img['src']
            # image_url_width = item.findNext('span', {'class': 'product-image'}).img['width']
            # image_url_height = item.findNext('span', {'class': 'product-image'}).img['height']
            # image = {'url': image_url, 'width': image_url_width, 'height': image_url_height}
            product_id = item.a['data-product-id']
            part_no = item.a['data-part-number']
            brand_name = item.a['data-ga-brand-name']
            brand_id = item.a['data-ga-brand-id']
            discount_price = float(re.sub(r'[^\d\.]', '', item.a['data-ga-discount-price']))
            out_of_stock = item.a['data-ga-is-out-of-stock']
            has_discount = item.a['data-ga-is-discontinued']
            inventory_status = item.a['data-ga-inventory-status']
            currency = item.findNext('meta', attrs={'itemprop': 'priceCurrency'})['content']
            price = item.findNext('meta', attrs={'itemprop': 'price'})['content']
            category = item.findNext('div', attrs={'itemprop': 'category'})['content']
            products.append(
                Product(url=url, name=name, rating=rating, nb_reviews=nb_reviews, image=None, product_id=product_id,
                        part_no=part_no, brand_name=brand_name, brand_id=brand_id,
                        discount_price=discount_price, out_of_stock=out_of_stock, has_discount=has_discount,
                        inventory_status=inventory_status, currency=currency, price=price, category=category))
        return products


class Product:
    def __init__(self, *, url, name, rating, nb_reviews, image, product_id, part_no, brand_name, brand_id,
                 discount_price, out_of_stock, has_discount, inventory_status, currency, price, category):
        self.url = url
        self.name = name
        self.rating = rating
        self.nb_reviews = nb_reviews
        self.image = image
        self.product_id = product_id
        self.part_no = part_no
        self.brand_name = brand_name
        self.brand_id = brand_id
        self.discount_price = discount_price
        self.out_of_stock = out_of_stock
        self.has_discount = has_discount
        self.inventory_status = inventory_status
        self.currency = currency
        self.price = price
        self.category = category


def get_page_content(url):
    headers = {"User-Agent": UA.random}
    response = requests.get(url, headers=headers)
    if response.ok:
        print(f'We could access the URL {url}')
    else:
        print(f'We could access the URL but we did not get the content of this URL {url}')
    html = response.content
    return html


def get_html_with_grequests(url):
    response = grequests.get(url)
    content = grequests.map([response])[0].content
    return content.decode('utf-8')


if __name__ == '__main__':
    req = Request_iherb()
    print(req.url_list)
    first_html = get_html_with_grequests(req.url_list[0])
    results = req.get_detailed_information_from_html(first_html)
    print(len(results))
