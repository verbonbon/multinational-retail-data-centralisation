from database_utils import DatabaseConnector
from tabula import read_pdf
import psycopg2
import yaml
import pandas as pd
import requests
import json
import boto3
from botocore import UNSIGNED
from botocore.client import Config


class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, DatabaseConnector, table_name):
        """ This takes in an instance of
            the DatabaseConnector class (from database_utils)
            and the table name as an argument and
            returns a pandas DataFrame with user data
        """
        table_list = DatabaseConnector.list_db_tables()
        user_table_name = [name for name in table_list if 'user' in name]
        print(f'This is the table name with the word "user":\
              \n {user_table_name}')

        database_connector = DatabaseConnector.init_db_engine()
        query = (f'SELECT * FROM {table_name}')
        user_data = pd.read_sql_query(query, database_connector)
        return user_data

    def retrieve_pdf_data(self, pdf_link):
        """ This takes in a link to a pdf file with card details as an argument
        and returns a pandas DataFrame
        link: https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf
        """
        pdf_file = read_pdf(pdf_link, pages='all')
        card_data = pd.concat(pdf_file, ignore_index=True)
        return card_data

    def list_number_of_stores(self):
        """This uses number of stores endpoint
        and dictionary with api key 
        and returns information about number of stores
        """
        number_of_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        header_api_key = {'x-api-key':
                          'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        number_of_stores_dict = requests.get(number_of_stores_url,
                                             headers=header_api_key).json()
        number_of_stores = number_of_stores_dict.get("number_stores")
        return number_of_stores
        # output: {'statusCode': 200, 'number_stores': 451}

    def retrieve_stores_data(self, number_of_stores):
        """This takes in endpoint (on number of stores) as an argument,
        extract all the stores from the API,
        and returns a pandas DataFrame about the stores
        """
        retrieve_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        header_api_key = {'x-api-key':
                          'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        stores_df = []
        for each_store in range(number_of_stores):
            extract_all_stores = requests.get(retrieve_stores_url +
                                              f'{each_store}',
                                              headers=header_api_key).json()
            column_heading = extract_all_stores.keys()
            stores_df.append(list(extract_all_stores.values()))
        stores_data = pd.DataFrame((stores_df), columns=column_heading)
        return stores_data

    def extract_from_s3(self):
        """This downloads and extracts a document from an S3 bucket on AWS.
        It returns a pandas DataFrame about the products
        The address is: s3://data-handling-public/products.csv
        """
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        s3_products = s3.get_object(Bucket='data-handling-public',
                                    Key='products.csv')
        products_data = pd.read_csv(s3_products['Body'])
        return products_data

    def sales_json_date(self):
        """This extract data from AWS S3
        and returns data about when each sale happened, and related attributes.
        """
        sales_json_url = 'http://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        sales_date = requests.get(sales_json_url).json()
        sales_date_data = pd.DataFrame(sales_date)
        return sales_date_data

# Testing within this file

    def test_code():
        """This tests all the methods in the class
        and returns the first 5 lines of each dataframe
        """
        database_connector = DatabaseConnector()
        # engine = database_connector.init_db_engine()
        data_extractor = DataExtractor()
        table_name = 'legacy_users'
        user_data_test = data_extractor.read_rds_table(database_connector,
                                                       table_name)
        print(f'Here is first 5 lines of the user data:\
              \n {user_data_test.head(5)}')

        pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        card_data_test = data_extractor.retrieve_pdf_data(pdf_link)
        print(f'Here is first 5 lines of the card data:\
              \n {card_data_test.head(5)}')

        number_of_stores = data_extractor.list_number_of_stores()
        stores_data_test = data_extractor.\
            retrieve_stores_data(number_of_stores)
        print(f'Here is first 5 lines of the stores data:\
              \n {stores_data_test.head(5)}')

        product_data_test = data_extractor.extract_from_s3()
        print(f'Here is first 5 lines of the product data:\
              \n {product_data_test.head(5)}')

        sales_date_test = data_extractor.sales_json_date()
        print(f'Here is first 5 lines of the sales data:\
              \n {sales_date_test.head(5)}')


if __name__ == '__main__':
    testing = DataExtractor.test_code()
    print(testing)
