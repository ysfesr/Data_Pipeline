import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

aws_key = config.get("AWS", "AWS_KEY")
aws_secret_key = config.get("AWS", "AWS_SECRET_KEY")

# CREATE STAGING TABELS

staging_customer_statut = """
CREATE TABLE IF NOT EXISTS Customer_Statut(
            statut_id INT NOT NULL PRIMARY KEY,
            statut VARCHAR(255) NOT NULL
)
"""

staging_customer = """
CREATE TABLE IF NOT EXISTS Customer(
            customer_id INT NOT NULL PRIMARY KEY,
            statut_id INT NOT NULL,
            individual_or_organization VARCHAR(50) NOT NULL,
            organisation_name varchar(50),
            individual_first_name varchar(50),
            individual_last_name VARCHAR(50),
)
"""

staging_order = """
CREATE TABLE IF NOT EXISTS Orders(
            order_id INT NOT NULL PRIMARY KEY,
            customer_id INT NOT NULL,
            amount_due INT NOT NULL,
            )
"""

staging_car_manufacturer = """
CREATE TABLE IF NOT EXISTS Car_Manufacturer(
            car_manufacturer_id INT NOT NULL PRIMARY KEY,
            name VARCHAR(50) NOT NULL
            )
"""

staging_car = """
CREATE TABLE IF NOT EXISTS Car(
            car_id INT NOT NULL PRIMARY KEY,
            car_manufacturer_id INT NOT NULL,
            date_of_manufacture DATE NOT NULL,
            model VARCHAR(50) NOT NULL
            )
"""

staging_supplier = """
CREATE TABLE IF NOT EXISTS Supplier(
            supplier_id INT NOT NULL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            street_address VARCHAR(50) NOT NULL,
            town VARCHAR(50) NOT NULL,
            country VARCHAR(50) NOT NULL,
            postcode INT NOT NULL,
            phone VARCHAR(50) NOT NULL
            )
"""

staging_brand = """
CREATE TABLE IF NOT EXISTS Brand(
            brand_id INT NOT NULL PRIMARY KEY,
            name VARCHAR(50) NOT NULL
            )
"""

staging_part_maker = """
CREATE TABLE IF NOT EXISTS Part_Maker(
            part_maker_id INT NOT NULL PRIMARY KEY,
            name VARCHAR(50) NOT NULL
            )
"""

staging_part = """
CREATE TABLE IF NOT EXISTS Part(
        part_id INT NOT NULL PRIMARY KEY,
        brand_id INT NOT NULL,
        supplier_id INT NOT NULL,
        part_group_id INT NOT NULL,
        part_maker_id INT NOT NULL,
        part_name VARCHAR(50) NOT NULL,
        main_supplier_name VARCHAR(50) NOT NULL,
        price_to_us INT NOT NULL,
        price_to_customer INT NOT NULL,

)
"""

staging_part_for_car = """
CREATE TABLE IF NOT EXISTS Part_for_Car(
            car_id INT NOT NULL,
            part_id INT NOT NULL,
            )
"""

staging_part_supplier = """
CREATE TABLE IF NOT EXISTS Part_Supplier(
        part_supplier_id INT NOT NULL PRIMARY KEY,
        part_id INT NOT NULL,
        supplier_id INT NOT NULL,
)
"""

staging_part_in_order = """
CREATE TABLE IF NOT EXISTS Part_in_Order(
            part_in_order_id INT NOT NULL,
            order_id INT NOT NULL,
            part_supplier_id INT NOT NULL,
            actual_sale_price INT NOT NULL,
            quantity INT NOT NULL,
)
"""

# DROP TABLES

drop_part_in_order = "DROP TABLE IF EXISTS Part_in_Order"
drop_supplier = "DROP TABLE IF EXISTS Supplier"
drop_brand = "DROP TABLE IF EXISTS Brand"
drop_part = "DROP TABLE IF EXISTS Part"
drop_part_for_car = "DROP TABLE IF EXISTS Part_for_Car"
drop_part_supplier = "DROP TABLE IF EXISTS Part_Supplier"
drop_customer = "DROP TABLE IF EXISTS Customer"
drop_customer_statut = "DROP TABLE IF EXISTS Customer_Statut "
drop_orders = "DROP TABLE IF EXISTS Orders"
drop_car_manufacturer = "DROP TABLE IF EXISTS Car_Manufacturer"
drop_car = "DROP TABLE IF EXISTS Car"
drop_part_maker = "DROP TABLE IF EXISTS Part_Maker"

# DIMENSIONAL TABLES

dim_brand = """ 
CREATE TABLE IF NOT EXISTS DimBrand(
        brand_id INT NOT NULL,
        name VARCHAR(50) NOT NULL
)
"""

dim_car = """
CREATE TABLE IF NOT EXISTS DimCar(
        car_id INT NOT NULL,
        car_manufacturer_id INT NOT NULL,
        date_of_manufacturer DATE NOT NULL,
        model VARCHAR(50) NOT NULL
)
"""

dim_car_manufacturer = """
CREATE TABLE IF NOT EXISTS DimCar_Manufacturer(
        car_manufacturer_id INT NOT NULL,
        name VARCHAR(50) NOT NULL
)
"""

dim_customer = """
CREATE TABLE IF NOT EXISTS DimCustomer(
        customer_id INT NOT NULL,
        statut_id INT NOT NULL,
        individual_or_organization VARCHAR(50) NOT NULL,
        organisation_name varchar(50),
        individual_first_name varchar(50),
        individual_last_name VARCHAR(50) 
)
"""

dim_order = """
CREATE TABLE IF NOT EXISTS DimOrder(
        order_id INT NOT NULL,
        customer_id INT NOT NULL,
        amount_due INT NOT NULL
)
"""

dim_supplier = """
CREATE TABLE IF NOT EXISTS DimSupplier(
        supplier_id INT NOT NULL,
        name VARCHAR(50) NOT NULL,
        street_address VARCHAR(50) NOT NULL,
        town VARCHAR(50) NOT NULL,
        country VARCHAR(50) NOT NULL,
        postcode INT NOT NULL,
        phone VARCHAR(50) NOT NULL
)
"""

dim_part_supplier = """
CREATE TABLE IF NOT EXISTS DimPart_Supplier(
        part_supplier_id INT NOT NULL,
        part_id INT NOT NULL,
        supplier_id INT NOT NULL
)
"""

dim_part_maker = """
CREATE TABLE IF NOT EXISTS DimPart_Maker(
        part_maker_id INT NOT NULL,
        name VARCHAR(50) NOT NULL
)
"""

dim_part_for_car = """
CREATE TABLE IF NOT EXISTS DimPart_for_Car(
        part_id INT NOT NULL,
        car_id INT NOT NULL
)
"""

dim_part = """
CREATE TABLE IF NOT EXISTS DimPart(
        part_id INT NOT NULL,
        brand_id INT NOT NULL,
        supplier_id INT NOT NULL,
        part_group_id INT NOT NULL,
        part_maker_id INT NOT NULL,
        part_name VARCHAR(50) NOT NULL,
        main_supplier_name VARCHAR(50) NOT NULL,
        price_to_us INT NOT NULL,
        price_to_customer INT NOT NULL
)
"""

fact_part_in_order = """
CREATE TABLE IF NOT EXISTS FactPart_in_Order(
        fact_id INT NOT NULL,
        brand_id INT NOT NULL,
        car_id INT NOT NULL,
        car_manufacturer INT NOT NULL,
        customer_id INT NOT NULL,
        order_id INT NOT NULL,
        part_id INT NOT NULL,
        part_maker INT NOT NULL,
        part_supplier INT NOT NULL,
        supplier_id INT NOT NULL,
        actual_sale_price INT NOT NULL,
        quantity INT NOT NULL
)
"""

# LOAD DATA TO STAGING TABLES

staging_customer_statut_copy = """
COPY staging_customer_statut FROM 's3://car_part_5364/customer_statut.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_customer_copy = """
COPY staging_customer FROM 's3://car_part_5364/customer.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_order_copy = """
COPY staging_order FROM 's3://car_part_5364/order.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_car_manufacturer_copy = """
COPY staging_car_manufacturer FROM 's3://car_part_5364/car_manufacturer.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_car_copy = """
COPY staging_car FROM 's3://car_part_5364/car.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_supplier_copy = """
COPY staging_supplier FROM 's3://car_part_5364/supplier.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_brand_copy = """
COPY staging_brand FROM 's3://car_part_5364/brand.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_part_copy = """
COPY staging_part FROM 's3://car_part_5364/part.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_part_maker_copy = """
COPY staging_part_maker FROM 's3://car_part_5364/part_maker.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_part_for_car_copy = """
COPY staging_part_for_car FROM 's3://car_part_5364/part_for_car.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_part_supplier_copy = """
COPY staging_part_supplier FROM 's3://car_part_5364/part_supplier.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

staging_part_in_order_copy = """
COPY staging_part_in_order FROM 's3://car_part_5364/part_in_order.csv'
CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
""".format(aws_key, aws_secret_key)

# TRANSFORMATION OF DATA

brand_insert = """
INSERT INTO dwh.DimBrand
SELECT * FROM Brand
"""

car_insert = """
INSERT INTO dwh.DimCar
SELECT * FROM Car
"""

customer_insert = """
INSERT INTO dwh.DimCustomer
SELECT * FROM Customer
"""

order_insert = """
INSERT INTO dwh.DimOrder
SELECT * FROM Orders
"""

supplier_insert = """
INSERT INTO dwh.DimSupplier
SELECT * FROM Supplier
"""

part_supplier_insert = """
INSERT INTO dwh.DimPart_Supplier
SELECT * FROM Part_Supplier
"""

part_maker_insert = """
INSERT INTO dwh.DimPart_Maker
SELECT * FROM Part_Maker
"""

part_for_car = """
INSERT INTO dwh.DimPart_for_Car
SELECT * FROM Part_for_Car
"""

part_insert = """
INSERT INTO dwh.DimPart
SELECT * FROM Part
"""

fact_part_in_order = """
INSERT INTO dwh.FactPart_in_Order (fact_id,brand_id,car_id,car_manufacturer_id,customer_id, order_id,part_id,
                                  part_maker_id, part_supplier_id, supplier_id, actual_sale_price, quantity)
SELECT a.part_in_order_id, d.brand_id, e.car_id, f.car_manufacturer_id, b.customer_id, a.order_id, c.part_id,
       d.part_maker_id, a.part_supplier_id, d.supplier_id, a.actual_sale_price, a.quantity
FROM Part_in_Order a
JOIN Orders b USING(order_id)
JOIN Part_Supplier c USING(part_supplier_id)
JOIN Part d USING(part_id)
JOIN Part_for_Car e USING(part_id)
JOIN Car f USING(car_id)
"""

# QUERIES LISTS

drop_tables_queries = [drop_customer_statut, drop_customer, drop_orders, drop_car_manufacturer, drop_car,\
                       drop_supplier, drop_brand, drop_part_maker, drop_part, drop_part_for_car, drop_part_supplier, \
                       drop_part_in_order]

stagin_tables_queries = [staging_customer_statut, staging_customer, staging_order, staging_car_manufacturer,\
                         staging_car, staging_supplier, staging_brand, staging_part_maker, staging_part,\
                         staging_part_for_car, staging_part_supplier, staging_part_in_order]


dwh_tables_queries = [dim_brand, dim_car, dim_car_manufacturer, dim_customer, dim_order, dim_supplier, dim_part_supplier,\
                      dim_part_maker, dim_part_for_car, dim_part, fact_part_in_order]

copy_tables_queries = [staging_customer_statut_copy, staging_customer_copy, staging_order_copy, staging_car_manufacturer_copy,\
                 staging_car_copy, staging_supplier_copy, staging_brand_copy, staging_part_maker_copy, staging_part_copy,\
                 staging_part_for_car_copy, staging_part_supplier_copy, staging_part_in_order_copy]

insert_tables_queries = [brand_insert, car_insert, customer_insert, order_insert, supplier_insert, part_supplier_insert,\
                  part_maker_insert, part_for_car, part_insert, fact_part_in_order]