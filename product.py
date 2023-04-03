class Product:
    """
    A class to represent a product on the iHerb website.

    Attributes
    ----------
    url : str
        The URL of the product page.
    name : str
        The name of the product.
    rating : float
        The rating of the product, on a scale from 0 to 5.
    nb_reviews : int
        The number of reviews for the product.
    image : dict
        A dictionary containing information about the product image.
    product_id : str
        The product ID.
    part_no : str
        The product part number.
    brand_name : str
        The name of the brand.
    brand_id : str
        The ID of the brand.
    discount_price : float
        The discounted price of the product.
    out_of_stock : bool
        True if the product is out of stock, False otherwise.
    has_discount : bool
        True if the product has a discount, False otherwise.
    inventory_status : str
        The inventory status of the product.
    currency : str
        The currency of the product price.
    price : float
        The price of the product.
    category : str
        The category of the product.

    Methods
    -------
    __str__():
        Returns a string representation of the product.
    """

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

    def __str__(self):
        return f"""{self.name} ({self.url}) - rating: {self.rating}, nb_reviews: {self.nb_reviews}, 
        price: {self.price} {self.currency}"""
