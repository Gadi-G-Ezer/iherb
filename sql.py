import logging

import pymysql

INSERT_CATEGORY = 'INSERT INTO CATEGORY (category,description) VALUES ("{cat}","");'
INSERT_BRAND = 'INSERT INTO BRANDS (name) VALUES ("{brand}");'
INSERT_STATUS = 'INSERT INTO INVENTORY_STATUS (state) VALUES ("{status}");'
COUNT_PRODUCT_WITH_IHERB_ID = 'SELECT COUNT(*) AS NUM_RESULT FROM product WHERE product.iherb_product_id={product_id};'
SELECT_BRAND_ID = "SELECT id FROM brands WHERE brands.name='{brand_name}'"
INSERT_PRODUCT = "INSERT INTO `product` (`iherb_product_id`, `url`, `name`, `rating`, `number_reviews`,`part_no`, " \
                 "`brand_id`, `discount_price`, `out_of_stock`, `inventory_status_id`, `currency`, `price`) VALUES ({" \
                 "product_id}, '{url}', '{prod_name}', {rating}, {nb_reviews},'{part_no}'," \
                 "{brand_id},{discount_price}, {out_of_stock}, {inventory_status},'{currency}', " \
                 "{price});"
SELECT_BRAND_WITH_NAME = "SELECT id FROM brands WHERE brands.name={brand_name}"


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
                request_product = f"""INSERT INTO `product` (`iherb_product_id`, `url`, `name`, `rating`, 
                `number_reviews`,`part_no`, `brand_id`, `discount_price`, `out_of_stock`, `inventory_status_id`, 
                `currency`, `price`)
                VALUES ({prod.product_id}, '{prod.url}', '{prod_name}', {prod.rating}, {prod.nb_reviews},
                '{prod.part_no}',{brand_id},{prod.discount_price}, {prod.out_of_stock}, {prod.inventory_status},
                '{prod.currency}', {prod.price});"""
                curs.execute(request_product)
                success = True
            except Exception as e:
                logging.info(f"""The product {str(prod)} has not been inserted into DB. Cause {e}""")
        elif num_result == 1:
            try:
                curs.execute(
                    f"""UPDATE product SET number_reviews ={prod.nb_reviews}, rating ={prod.rating} WHERE 
                    iherb_product_id = {prod.product_id};""")
                success = True
            except Exception as e:
                logging.info(
                    f"""FAIL : UPDATE product SET number_reviews = {prod.nb_reviews}, rating = {prod.rating} 
                    WHERE iherb_product_id = {prod.product_id}; CAUSE : {e}""")
                print(f"UPDATE FAILED for {prod.product_id} !!!")
        else:
            raise 'Houston we have a problem'

        # If we were able to insert the product, we populate product_category table
        if success:
            curs.execute(f"""SELECT id FROM category WHERE category.category='{prod.category}'""")
            result = curs.fetchall()
            category_id = result[0]['id']
            curs.execute(f"""SELECT id FROM product WHERE product.iherb_product_id='{prod.product_id}'""")
            result = curs.fetchall()
            product_id = result[0]['id']
            try:
                curs.execute(f"""INSERT INTO `product_category` (`product_id`, `category_id`) VALUES ({product_id},
                             {category_id})""")
            except Exception as e:
                logging.info(
                    f"""FAIL : INSERT INTO `product_category` (`product_id`, `category_id`) VALUES ({product_id},
                             {category_id});  CAUSE = {e}""")
