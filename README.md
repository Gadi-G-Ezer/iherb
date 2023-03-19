# iHerb Web Scraper

This project is a web scraper that collects product information from the [iHerb](https://www.iherb.com) website. The scraper uses `requests`, `grequests`, and `BeautifulSoup` to extract data from the website.

## Usage

To use the scraper, simply import the `Request_iherb` class from the `iherb_scraper` module and create an instance of it. You can then call the `get_detailed_information_from_html` method to extract product information from a given HTML content.

```python
from iherb_scraper import Request_iherb

# Create an instance of Request_iherb
request_iherb = Request_iherb()

# Get HTML content for a specific page
html_content = get_page_content('https://www.iherb.com/c/sports')

# Extract product information from HTML content
products = request_iherb.get_detailed_information_from_html(html_content)

# Print product information
for product in products:
    print(product.name, product.price)
