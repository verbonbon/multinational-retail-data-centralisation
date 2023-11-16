from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd
pd.options.display.max_columns = None
pd.options.display.max_rows = None

database_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()

# generate clean user data
table_name = 'legacy_users'
user_data = data_extractor.read_rds_table(database_connector,
                                          table_name)
user_data_clean = data_cleaning.clean_user_data(user_data)

# generate clean card data
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_data = data_extractor.retrieve_pdf_data(pdf_link)
card_data_clean = data_cleaning.clean_card_data(card_data)

# generate clean store data
number_of_stores = data_extractor.list_number_of_stores()
stores_data = data_extractor.retrieve_stores_data(number_of_stores)
store_data_clean = data_cleaning.clean_store_data(stores_data)

# generate clean product data
product_data = data_extractor.extract_from_s3()
product_weight_converted = data_cleaning.\
    convert_product_weights(product_data)
product_data_clean = data_cleaning.\
    clean_products_data(product_weight_converted)

# generate clean order data
order_data_clean = data_cleaning.clean_orders_data()

# generate clean date data
sales_date = data_extractor.sales_json_date()
sales_date_clean = data_cleaning.clean_sales_date(sales_date)

# upload the data to SQL database
user_upload = database_connector.upload_to_db(user_data_clean,
                                              'user_data_clean')
card_upload = database_connector.upload_to_db(card_data_clean,
                                              'card_data_clean')
card_upload = database_connector.upload_to_db(card_data_clean,
                                              'card_data_clean')
store_upload = database_connector.upload_to_db(store_data_clean,
                                               'store_data_clean')
product_upload = database_connector.upload_to_db(product_data_clean,
                                                 'product_data_clean')
order_upload = database_connector.upload_to_db(order_data_clean,
                                               'order_data_clean')
dates_upload = database_connector.upload_to_db(sales_date_clean,
                                               'sales_date_clean')
