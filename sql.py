import logging
import pymysql.cursors
import json


class SQLQueries:
    """A class that loads SQL queries from a JSON file and sets them as attributes.

    Args:
        json_file (str): The file path of the JSON file containing the SQL queries.

    Attributes:
        queries (dict): A dictionary containing the loaded SQL queries.
    """
    def __init__(self, json_file):
        with open(json_file, 'r') as f:
            self.queries = json.load(f)
        for key, value in self.queries.items():
            setattr(self, key, value)


sql_ = SQLQueries('SQL_queries.json')


def connect_to_pymysql(func):
    """
    A decorator that connects to a MySQL database and passes a cursor object to the decorated function.

    Args:
        func (callable): The function to decorate.

    Returns:
        callable: The decorated function.

    Raises:
        Any error that can be raised by pymysql.connect().

    Example:
        @connect_to_pymysql
        def my_function(curs, sql_queries, arg1, arg2, kwarg1=None, kwarg2=None):
            # Use the curs object to execute SQL queries and the sql_queries object to access pre-defined queries.
            # ...

    Notes:
        This decorator reads the connection parameters from a 'conf.json' file located in the current working
        directory. The 'conf.json' file should have the following format:

        {
            "DATABASE": {
                "HOST": "localhost",
                "USER": "my_user",
                "PASSWORD": "mypassword",
                "DB": "my_database",
                "CHARSET": "utf8mb4"
            }
        }

        The 'func' parameter should accept at least two parameters: 'curs' and 'sql_queries'. 'curs' is a pymysql
        cursor object that can be used to execute SQL queries. 'sql_queries' is an instance of the SQLQueries class
        defined in the 'sql_queries.py' module, which can be used to access pre-defined SQL queries stored in a JSON
        file.

        This decorator automatically commits changes to the database in case of SQL queries that modify the data
        (e.g. INSERT, UPDATE, DELETE). If an error occurs during the commit, the changes are rolled back.

        The connection and cursor objects are closed automatically after the decorated function is executed.

        This decorator ensures that a new connection to the database is created every time a function is decorated.
    """
    def wrapper(*args, **kwargs):
        # get parameters from configurations file
        with open('conf.json', 'r') as f:
            config = json.load(f)
        # create a connection to the MySQL database
        connection = pymysql.connect(
            host=config['DATABASE']['HOST'],
            user=config['DATABASE']['USER'],
            password=config['DATABASE']['PASSWORD'],
            db=config['DATABASE']['DB'],
            charset=config['DATABASE']['CHARSET'],
            cursorclass=pymysql.cursors.DictCursor
        )
        # create a cursor object from the connection
        curs = connection.cursor()
        # pass the cursor as a parameter to the decorated function
        result = func(curs, *args, **kwargs)
        # Commit changes to the database in case of sql queries such as: INSERT, UPDATE, or DELETE.
        connection.commit()
        # close the cursor and connection after the function call is completed
        curs.close()
        connection.close()
        return result
    return wrapper


@connect_to_pymysql
def insert_categories_into_db(curs, products):
    """
    inserts the categories of the products into the DB
    :param products: List of products
    :param curs: curso object
    :return: None
    """
    global sql_
    categories = {p.category for p in products}
    for cat in categories:
        try:
            curs.execute(sql_.INSERT_CATEGORY.format(cat=cat))
        except pymysql.err.IntegrityError as err:
            logging.info(err)


@connect_to_pymysql
def insert_brands_into_db(curs, products):
    """
    inserts the brands of the products into the DB
    :param products: List of products
    :param curs: curso object
    :return: None
    """
    global sql_
    brands = {p.brand_name for p in products}
    for brand in brands:
        try:
            curs.execute(sql_.INSERT_BRAND.format(brand=brand))
        except pymysql.err.IntegrityError as err:
            logging.info(err)


@connect_to_pymysql
def insert_inventory_status_into_db(curs, products):
    """
    inserts the inventory status of the products into the DB
    :param products: List of products
    :param curs: curso object
    :return: None
    """
    global sql_
    inventory_status = {p.inventory_status for p in products}
    for status in inventory_status:
        try:
            curs.execute(sql_.INSERT_STATUS.format(status=status))
        except pymysql.err.IntegrityError as err:
            logging.info(err)


@connect_to_pymysql
def insert_product_into_db(curs, products):
    """
    inserts the products into the DB
    :param products: List of products
    :param curs: curso object
    :return: None
    """
    global sql_
    for prod in products:
        curs.execute(sql_.SELECT_PRODUCT_ID_USING_IHERB_ID.format(product_id=prod.product_id))
        if curs.fetchone() is None:
            create_new_product(prod)
        else:
            update_existing_product(prod)

        populate_product_category(prod)


@connect_to_pymysql
def create_new_product(curs, prod):
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
    global sql_
    try:
        brand_name = '"' + prod.brand_name + '"'
        curs.execute(sql_.SELECT_BRAND_WITH_NAME.format(brand_name=brand_name))
        result = curs.fetchall()
        brand_id = result[0]['id']
        prod_name = prod.name.replace("'", " ")
        curs.execute(sql_.INSERT_INTO_PRODUCT.format(product_id=prod.product_id, url=prod.url, prod_name=prod_name,
                                                     rating=prod.rating, nb_reviews=prod.nb_reviews,
                                                     part_no=prod.part_no, brand_id=brand_id,
                                                     discount_price=prod.discount_price, out_of_stock=prod.out_of_stock,
                                                     inventory_status=prod.inventory_status, currency=prod.currency,
                                                     price=prod.price))
    except pymysql.err.Error as e:
        logging.info(f"""The product {str(prod)} has not been inserted into DB. Cause {e}""")


@connect_to_pymysql
def update_existing_product(curs, prod):
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
    global sql_
    try:
        curs.execute(
            sql_.UPDATE_PRODUCT.format(nb_reviews=prod.nb_reviews, rating=prod.rating, product_id=prod.product_id))
    except pymysql.err.Error as e:
        logging.info(
            f"""FAIL : {sql_.UPDATE_PRODUCT.format(nb_reviews=prod.nb_reviews, rating=prod.rating,
                                              product_id=prod.product_id)} CAUSE : {e}""")
        print(f"UPDATE FAILED for {prod.product_id} !!!")


@connect_to_pymysql
def populate_product_category(curs, prod):
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
    global sql_
    curs.execute(sql_.SELECT_CATEGORY_ID.format(category=prod.category))
    result = curs.fetchall()
    category_id = result[0]['id']
    curs.execute(sql_.SELECT_PRODUCT_ID_USING_IHERB_ID.format(product_id=prod.product_id))
    result = curs.fetchall()
    product_id = result[0]['id']
    try:
        curs.execute(sql_.INSERT_PRODUCT_CATEGORY.format(product_id=product_id, category_id=category_id))
    except pymysql.err.Error as e:
        logging.info(
            f"""FAIL : {sql_.INSERT_PRODUCT_CATEGORY.format(product_id=product_id, category_id=category_id)}
                        CAUSE = {e}""")


@connect_to_pymysql
def get_brands_names(curs, products):
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
    global sql_
    brands_list = list(set([f'"{str(product.brand_name)}"' for product in products]))
    brands = ",".join(brands_list)
    try:
        curs.execute(sql_.SELECT_BRANDS_FROM_REQ.format(brands=brands))
    except BaseException as error:
        print("ERROR : ", error)
        logging.error(f"Error: {error}")
    brands = curs.fetchall()
    return brands


@connect_to_pymysql
def update_number_tweets(curs, brands):
    """
    Update the number of tweets for each brand in the database.

    Args:
        brands (list): A list of dictionaries, where each dictionary represents a brand and contains the keys 'id'
            and 'tweets_qty'.
        curs: The cursor object used to execute the SQL queries.

    Returns:
        None

    Raises:
        pymysql.err.Error: If an error occurs while executing the SQL query.

    Example usage:
        brands = [{'id': 1, 'name'='toto', 'tweets_qty': 100}, {'id': 2,'name'='tata', 'tweets_qty': 200}]
        curs = conn.cursor()
        update_number_tweets(brands, curs)
    """
    global sql_
    for brand in brands:
        id_ = brand['id']
        tweets_qty = brand['number_of_tweets']
        try:
            curs.execute(sql_.UPDATE_BRAND_TWEETS_QTY.format(tweets=tweets_qty, brand_id=id_))
        except pymysql.err.Error as e:
            logging.info(
                f"""FAIL : {sql_.UPDATE_BRAND_TWEETS_QTY.format(tweets=tweets_qty, brand_id=id_)} CAUSE : {e}""")
