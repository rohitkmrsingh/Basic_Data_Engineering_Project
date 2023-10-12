#!/usr/bin/env python
# coding: utf-8

# In[1]:


## pip install psycopg2
## pip install pandas

import psycopg2
import pandas as pd


## creating a new database
conn= psycopg2.connect(host='127.0.0.1',port ='5432',database='postgres',user='postgres',password='Qwerty@1234#')
conn.autocommit=True

##cursor
cursor = conn.cursor()

## creating database
bike_db = "create Database bike_store"

##executing the above query
cursor.execute(bike_db)
print("Database has been created successfully!")

## closing the connection
conn.close()

## creating tables in the database
def create_tables(): 
    """ create tables in the PostgreSQL database"""
    commands = (
    '''CREATE TABLE if not exists brands (
        brand_id SERIAL NOT NULL,
        brand_name VARCHAR(255),
        PRIMARY KEY (brand_id)
    )''',
    '''CREATE TABLE if not exists categories (
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(50)
    )''',
    '''CREATE TABLE if not exists customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        phone BIGINT,
        email VARCHAR(50) NOT NULL,
        street VARCHAR(255),
        city VARCHAR(50),
        state VARCHAR(10),
        zip_code INTEGER
    )''',
    '''CREATE TABLE if not exists stores (
        store_id SERIAL PRIMARY KEY,
        store_name VARCHAR(255) NOT NULL,
        phone BIGINT,
        email VARCHAR(50),
        street VARCHAR(255),
        city VARCHAR(30),
        state VARCHAR(30),
        zip_code INTEGER
    )''',
    '''CREATE TABLE if not exists staffs (
        staff_id SERIAL PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        email VARCHAR(50),
        phone BIGINT,
        active BOOLEAN,
        store_id INTEGER,
        manager_id INTEGER,
        FOREIGN KEY(manager_id) REFERENCES staffs(staff_id) ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY(store_id) REFERENCES stores(store_id) ON UPDATE CASCADE ON DELETE CASCADE
    )''',
    '''CREATE TABLE if not exists products (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(100),
        brand_id INTEGER,
        category_id INTEGER,
        model_year INTEGER,
        list_price FLOAT,
        FOREIGN KEY(brand_id) REFERENCES brands(brand_id) ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY(category_id) REFERENCES categories(category_id) ON UPDATE CASCADE ON DELETE CASCADE
    )''',
    '''CREATE TABLE if not exists orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        order_status INTEGER,
        order_date DATE,
        required_date DATE,
        shipped_date DATE,
        store_id INTEGER,
        staff_id INTEGER,
        FOREIGN KEY(customer_id) REFERENCES customers(customer_id) ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY(store_id) REFERENCES stores(store_id) ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY(staff_id) REFERENCES staffs(staff_id) ON UPDATE CASCADE ON DELETE CASCADE
    )''',
    '''CREATE TABLE if not exists order_items (
        order_id INTEGER NOT NULL,
        item_id SERIAL NOT NULL,
        product_id INTEGER,
        quantity INTEGER,
        list_price FLOAT,
        discount FLOAT,
        PRIMARY KEY(order_id, item_id),
        FOREIGN KEY(order_id) REFERENCES orders(order_id) ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY(product_id) REFERENCES products(product_id) ON UPDATE CASCADE ON DELETE CASCADE
    )''',
    '''CREATE TABLE if not exists stocks (
        store_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER,
        PRIMARY KEY(store_id, product_id),
        FOREIGN KEY(store_id) REFERENCES stores(store_id) ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY(product_id) REFERENCES products(product_id) ON UPDATE CASCADE ON DELETE CASCADE
    )'''
)

   
    conn = None
    try: 
        # connect to the PostgreSQL server 
        conn = psycopg2.connect(host='127.0.0.1',port ='5432',database='bike_store',user='postgres',password='Qwerty@1234#') 
        cur = conn.cursor() 
        # create table one by one 
        for command in commands: 
            cur.execute(command) 
        # close communication with the PostgreSQL database server 
        cur.close() 
        # commit the changes 
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error: 
        print(error) 
    finally: 
        if conn is not None: 
            conn.close() 
  
  
if __name__ == '__main__': 
    create_tables()
    
## cleaning the phone column in stores file
df = pd.read_csv('C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/stores.csv')
df['phone'] = df['phone'].str.replace(r'[-()\s]', '',regex=True)
df.to_csv('stores1.csv', index=False)

## cleaning the phone and manager_id column in staffs file 
df = pd.read_csv('C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/staffs.csv')
df['phone'] = df['phone'].str.replace(r'[-()\s]', '',regex=True)
df['manager_id'] = df['manager_id'].fillna(0).astype(int).replace(1, 1)
df.to_csv('staffs1.csv', index=False)

## inserting data into the tables created above

def insert_data(): 
    """ inserting data into the created tables in the PostgreSQL database"""
    commands = (
    '''COPY brands(brand_id,brand_name) 
FROM 'C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/brands.csv' 
DELIMITER ',' 
CSV HEADER;''',
    '''COPY categories(category_id,category_name) 
FROM 'C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/categories.csv' 
DELIMITER ',' 
CSV HEADER;''',
     '''COPY customers(customer_id,first_name,last_name,phone,email,street,city,state,zip_code) 
FROM 'C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/customers.csv' 
DELIMITER ',' 
CSV HEADER;''',
    '''COPY stores(store_id,store_name,phone,email,street,city,state,zip_code) 
FROM 'C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/stores1.csv' 
DELIMITER ','
CSV HEADER;''',
    '''COPY staffs(staff_id,first_name,last_name,email,phone,active,store_id,manager_id) 
FROM 'C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/staffs1.csv' 
DELIMITER ','
CSV HEADER;''',
    '''COPY products(product_id,product_name,brand_id,category_id,model_year,list_price) 
FROM 'C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/products.csv' 
DELIMITER ','
CSV HEADER;''',
    '''COPY orders(order_id,customer_id,order_status,order_date,required_date,shipped_date,store_id,staff_id) 
FROM 'C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/orders.csv' 
DELIMITER ','
CSV HEADER;''',
    '''COPY order_items(order_id,item_id,product_id,quantity,list_price,discount) 
FROM 'C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/order_items.csv' 
DELIMITER ','
CSV HEADER;''',
    '''COPY stocks(store_id,product_id,quantity) 
FROM 'C:/Users/rohit/Downloads/Data Engineering/Projects/Project Data/Bike_Store_relational_data/stocks.csv' 
DELIMITER ','
CSV HEADER;'''
)

    conn = None
    try: 
        # connect to the PostgreSQL server 
        conn = psycopg2.connect(host='127.0.0.1',port ='5432',database='bike_store',user='postgres',password='Qwerty@1234#') 
        cur = conn.cursor() 
        # create table one by one 
        for command in commands: 
            cur.execute(command) 
        # close communication with the PostgreSQL database server 
        cur.close() 
        # commit the changes 
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error: 
        print(error) 
    finally: 
        if conn is not None: 
            conn.close() 
  
  
if __name__ == '__main__': 
    insert_data()






