Bike Store Data Engineering Project

Welcome to the Bike Store Data Engineering Project repository! This project focuses on creating a robust relational database using PostgreSQL and Python. The foundation of our database is the Bike Store dataset sourced from Kaggle, encompassing various aspects of a typical bike store's operations. The dataset provides a rich source of information, allowing us to explore the intricate workings of the business, from customers and orders to products and inventory.

Dataset Information

The Bike Store dataset includes the following tables in our database:

customers:Information about the store's customers.
orders:Details of customer orders, including order date and status.
staffs: Data related to store staff, including names and contact information.
stores: Store-specific information such as location and store ID.
order_items:Items included in each order, linked to products and quantities.
categories:Categories of products available in the store.
products: Product details including name, category, and price.
stocks:Inventory details, including the number of products in stock.
brands: Information about the brands associated with the products.

 Database Schema
The tables in our PostgreSQL database have been designed to capture the intricacies of the bike store's operations. Here's a brief overview of the schema:

customers:(customer_id, first_name, last_name, email, phone, address, city, state, zip_code)
orders: (order_id, customer_id, order_date, status)
staffs: (staff_id, first_name, last_name, email, phone, address, city, state, zip_code)
stores: (store_id, store_name, location, phone)
order_items:(order_item_id, order_id, product_id, quantity, unit_price)
categories: (category_id, category_name)
products: (product_id, product_name, category_id, brand_id, model_year, list_price)
stocks: (product_id, store_id, quantity)
brands: (brand_id, brand_name)

Data Model

The data model for this project has been structured based on the Bike Store dataset available on Kaggle. The schema provides a clear and organized representation of the data, enabling efficient analysis and reporting.

Here is the data model, already provided in Kaggle, which served as the reference for our table creation:

![image](https://github.com/rohitkmrsingh/Basic_Data_Engineering_Project/assets/145882940/a49e3861-298a-463f-b73c-e3ef71e30747)




