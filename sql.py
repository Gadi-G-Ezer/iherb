import logging

import pymysql

# All the requests used in the program
INSERT_CATEGORY = 'INSERT INTO CATEGORY (category,description) VALUES ("{cat}","");'
INSERT_BRAND = 'INSERT INTO BRANDS (name) VALUES ("{brand}");'
INSERT_STATUS = 'INSERT INTO INVENTORY_STATUS (state) VALUES ("{status}");'
COUNT_PRODUCT_WITH_IHERB_ID = 'SELECT COUNT(*) AS NUM_RESULT FROM product WHERE product.iherb_product_id={product_id};'
SELECT_BRAND_ID = "SELECT id FROM brands WHERE brands.name='{brand_name}'"
SELECT_BRAND_WITH_NAME = "SELECT id FROM brands WHERE brands.name={brand_name}"
INSERT_INTO_PRODUCT = "INSERT INTO `product` (`iherb_product_id`, `url`, `name`, `rating`,`number_reviews`,`part_no`," \
                      "`brand_id`, `discount_price`, `out_of_stock`, `inventory_status_id`, `currency`, " \
                      "`price`) VALUES ({product_id}, '{url}', '{prod_name}', {rating}, " \
                      "{nb_reviews},'{part_no}',{brand_id},{discount_price}, {out_of_stock}, " \
                      "{inventory_status},'{currency}', {price});"
UPDATE_PRODUCT = "UPDATE product SET number_reviews ={nb_reviews}, rating ={rating} WHERE iherb_product_id " \
                 "= {product_id};"
SELECT_CATEGORY_ID = "SELECT id FROM category WHERE category.category='{category}'"
SELECT_PRODUCT_ID_USING_IHERB_ID = "SELECT id FROM product WHERE product.iherb_product_id='{product_id}'"
INSERT_PRODUCT_CATEGORY = "INSERT INTO `product_category` (`product_id`, `category_id`) VALUES ({product_id}," \
                          "{category_id})"
UPDATE_BRAND_TWEETS_QTY = "UPDATE brands SET number_of_tweets ={tweets} WHERE id={brand_id};"

def insert_categories_into_db(products, curs):
    """
    inserts the categories of the products into the DB
    :param products: List of products
    :param curs: curso object
    :return: None
    """
    categories = {p.category for p in products}
    for cat in categories:
        try:
            curs.execute(INSERT_CATEGORY.format(cat=cat))
        except pymysql.err.IntegrityError as err:
            logging.info(err)


def insert_brands_into_db(products, curs):
    """
    inserts the brands of the products into the DB
    :param products: List of products
    :param curs: curso object
    :return: None
    """
    brands = {p.brand_name for p in products}
    for brand in brands:
        try:
            curs.execute(INSERT_BRAND.format(brand=brand))
        except pymysql.err.IntegrityError as err:
            logging.info(err)


def insert_inventory_status_into_db(products, curs):
    """
    inserts the inventory status of the products into the DB
    :param products: List of products
    :param curs: curso object
    :return: None
    """
    inventory_status = {p.inventory_status for p in products}
    for status in inventory_status:
        try:
            curs.execute(INSERT_STATUS.format(status=status))
        except pymysql.err.IntegrityError as err:
            logging.info(err)


def insert_product_into_db(products, curs):
    """
    inserts the products into the DB
    :param products: List of products
    :param curs: curso object
    :return: None
    """
    for prod in products:
        curs.execute(SELECT_PRODUCT_ID_USING_IHERB_ID.format(product_id=prod.product_id))
        if curs.fetchone() is None:
            create_new_product(prod, curs)
        else:
            update_existing_product(prod, curs)

        populate_product_category(prod, curs)


def create_new_product(prod, curs):
    """
    Inserts a new product record into the database using the given product object and database cursor.

    Args:
        prod (Product): The product object to insert.
        curs: The database cursor to use for executing SQL queries.

    Returns:
        None

    Raises:
        pymysql.err.Error: If there is an error executing the SQL query.
    """
    try:
        brand_name = '"' + prod.brand_name + '"'
        curs.execute(SELECT_BRAND_WITH_NAME.format(brand_name=brand_name))
        result = curs.fetchall()
        brand_id = result[0]['id']
        prod_name = prod.name.replace("'", " ")
        curs.execute(INSERT_INTO_PRODUCT.format(product_id=prod.product_id, url=prod.url, prod_name=prod_name,
                                                rating=prod.rating, nb_reviews=prod.nb_reviews,
                                                part_no=prod.part_no, brand_id=brand_id,
                                                discount_price=prod.discount_price,
                                                out_of_stock=prod.out_of_stock,
                                                inventory_status=prod.inventory_status, currency=prod.currency,
                                                price=prod.price))
    except pymysql.err.Error as e:
        logging.info(f"""The product {str(prod)} has not been inserted into DB. Cause {e}""")


def update_existing_product(prod, curs):
    """
    Updates an existing product record in the database with the given product object and database cursor.

    Args:
        prod (Product): The product object to update.
        curs: The database cursor to use for executing SQL queries.

    Returns:
        None

    Raises:
        pymysql.err.Error: If there is an error executing the SQL query.
    """
    try:
        curs.execute(
            UPDATE_PRODUCT.format(nb_reviews=prod.nb_reviews, rating=prod.rating, product_id=prod.product_id))
    except pymysql.err.Error as e:
        logging.info(
            f"""FAIL : {UPDATE_PRODUCT.format(nb_reviews=prod.nb_reviews, rating=prod.rating,
                                              product_id=prod.product_id)} CAUSE : {e}""")
        print(f"UPDATE FAILED for {prod.product_id} !!!")


def populate_product_category(prod, curs):
    """
    Inserts a new product-category association record into the database using the given product object and
    database cursor.

    Args:
        prod (Product): The product object to associate with a category.
        curs: The database cursor to use for executing SQL queries.

    Returns:
        None

    Raises:
        pymysql.err.Error: If there is an error executing the SQL query.
    """
    curs.execute(SELECT_CATEGORY_ID.format(category=prod.category))
    result = curs.fetchall()
    category_id = result[0]['id']
    curs.execute(SELECT_PRODUCT_ID_USING_IHERB_ID.format(product_id=prod.product_id))
    result = curs.fetchall()
    product_id = result[0]['id']
    try:
        curs.execute(INSERT_PRODUCT_CATEGORY.format(product_id=product_id, category_id=category_id))
    except pymysql.err.Error as e:
        logging.info(
            f"""FAIL : {INSERT_PRODUCT_CATEGORY.format(product_id=product_id, category_id=category_id)}
                        CAUSE = {e}""")


def get_brands_names(products, curs):
    """
    Returns a list of dictionaries representing the brands of the given products.

    Args:
        products (List[Product]): A list of Product objects with brand names.
        curs (Cursor): A database cursor used to execute SQL queries.

    Returns:
        List[Dict[str, Union[int, str]]]: A list of dictionaries with the keys 'id' and 'name',
        representing the id and name of each brand. The list is ordered according to the order
        of the given products.

    Raises:
        DatabaseError: If there is an error executing the SQL query.

    Example:
        products = [Product(brand_name='Apple'), Product(brand_name='Samsung')]
        curs = conn.cursor()
        brands = get_brands_names(products, curs)
        # brands == [{'id': 1, 'name': 'Apple'}, {'id': 2, 'name': 'Samsung'}]
    """
    brand_list = list(set([product.brand_name for product in products]))
    brands = []
    for brand in brand_list:
        try:
            curs.execute(SELECT_BRAND_WITH_NAME.format(brand_name='"'+brand+'"'))
        except BaseException as error:
            print("ERROR : ",error)
            logging.error(f"Error: {error}")
        brands.append({'id': curs.fetchall()[0]['id'], 'name': brand})
    return brands


def update_number_tweets(brands_dict, curs):
    """
    Updates the number of tweets for each brand in the database.

    Args:
        brands_dict (Dict[int, int]): A dictionary where the keys are brand ids and the values are
        the number of tweets for each brand.
        curs (Cursor): A database cursor used to execute SQL queries.

    Returns:
        None

    Raises:
        DatabaseError: If there is an error executing the SQL query.

    Example:
        brands_dict = {1: 100, 2: 200, 3: 300}
        curs = conn.cursor()
        update_number_tweets(brands_dict, curs)
    """
    for id_, tweets_qty in brands_dict.items():
        try:
            curs.execute(UPDATE_BRAND_TWEETS_QTY.format(tweets=tweets_qty, brand_id=id_))
        except pymysql.err.Error as e:
            logging.info(
                f"""FAIL : {UPDATE_BRAND_TWEETS_QTY.format(tweets=tweets_qty, brand_id=id_)} CAUSE : {e}""")
            print(f"UPDATE FAILED for id: {id_}, number of tweets: {tweets_qty} !!!")


