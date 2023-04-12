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
        success = False
        curs.execute(COUNT_PRODUCT_WITH_IHERB_ID.format(product_id=prod.product_id))
        result = curs.fetchall()
        num_result = result[0]['NUM_RESULT']
        if num_result == 0:
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
                success = True
            except pymysql.err.Error as e:
                logging.info(f"""The product {str(prod)} has not been inserted into DB. Cause {e}""")
        elif num_result == 1:
            try:
                curs.execute(
                    UPDATE_PRODUCT.format(nb_reviews=prod.nb_reviews, rating=prod.rating, product_id=prod.product_id))
                success = True
            except pymysql.err.Error as e:
                logging.info(
                    f"""FAIL : {UPDATE_PRODUCT.format(nb_reviews=prod.nb_reviews, rating=prod.rating,
                                                      product_id=prod.product_id)} CAUSE : {e}""")
                print(f"UPDATE FAILED for {prod.product_id} !!!")
        else:
            raise 'Houston we have a problem'

        # If we were able to insert the product, we populate product_category table
        if success:
            curs.execute(SELECT_CATEGORY_ID.format(category=prod.category))
            result = curs.fetchall()
            category_id = result[0]['id']
            curs.execute(SELECT_PRODUCT_ID_USING_IHERB_ID.format(product_id=prod.product_id))
            result = curs.fetchall()
            product_id = result[0]['id']
            try:
                curs.execute(INSERT_PRODUCT_CATEGORY.format(product_id=product_id,category_id=category_id))
            except pymysql.err.Error as e:
                logging.info(
                    f"""FAIL : {INSERT_PRODUCT_CATEGORY.format(product_id=product_id,category_id=category_id)}  
                    CAUSE = {e}""")
