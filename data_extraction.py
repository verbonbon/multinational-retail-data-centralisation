# Create a new Python script named data_extraction.py and within it, create a class named DataExtractor.
# This class will work as a utility class, 
# in it you will be creating methods that help extract data from different data sources.
# The methods contained will be fit to extract data from a particular data source, 
# these sources will include CSV files, an API and an S3 bucket.

from sqlalchemy import create_engine
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from tabula import read_pdf
import tabula
import numpy as np
import psycopg2
import yaml
import pandas as pd
import requests
import json
import time
import boto3
import io
import os
from botocore import UNSIGNED
from botocore.client import Config


class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, DatabaseConnector, table_name):
        """ This takes in an instance of the DatabaseConnector class 
            and the table name as an argument and returns a pandas DataFrame containing user data
        """
        table_list = DatabaseConnector.list_db_tables()
        user_table_name = [name for name in table_list if 'user' in name]
        print(user_table_name)

        database_connector = DatabaseConnector.init_db_engine()
        query = (f'SELECT * FROM {table_name}')
        df_sql = pd.read_sql_query(query, database_connector)
        return df_sql

    def retrieve_pdf_data(self, pdf_link):
        """ 
        This takes in a link to a pdf file as an argument and returns a pandas DataFrame
        """
        pdf_file = read_pdf(pdf_link, pages='all')
        df_pdf = pd.concat(pdf_file, ignore_index=True)
        return df_pdf

    def list_number_of_stores(self):
        """
        This takes in number of stores endpoint and dictionary with api key as arguments
        and returns information about number of stores
        """
        number_of_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        header_api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        number_of_stores_dict = requests.get(number_of_stores_url, headers=header_api_key).json()
        number_of_stores = number_of_stores_dict.get("number_stores")
        return number_of_stores  # {'statusCode': 200, 'number_stores': 451}

    def retrieve_stores_data(self, number_of_stores):
        """
        This takes in endpoint (on number of stores) as an argument,
        extract all the stores from the API,
        and returns a pandas DataFrame about the stores
        """
        retrieve_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        header_api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        stores_data = []
        for each_store in range(number_of_stores):
            extract_all_stores = requests.get(retrieve_stores_url + f'{each_store}', headers=header_api_key).json()
            column_heading = extract_all_stores.keys()
            stores_data.append(list(extract_all_stores.values()))
        df_stores = pd.DataFrame((stores_data), columns=column_heading)
        return df_stores

    def extract_from_s3(self):
        """
        This downloads and extracts a document in an S3 bucket on AWS.
        It returns a pandas DataFrame
        The address is: s3://data-handling-public/products.csv
        """
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        s3_products = s3.get_object(Bucket='data-handling-public', Key='products.csv')
        df_products = pd.read_csv(s3_products['Body'])
        return df_products

    def sales_json_date(self):
      sales_json_url = 'http://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
      sales_date = requests.get(sales_json_url).json()
      sales_date_df = pd.DataFrame(sales_date) 
      return sales_date_df

database_connector = DatabaseConnector()
# pdf_link1 = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"


#data_extractor = DataExtractor()
#rds_table = data_extractor.read_rds_table(database_connector, 'orders_table')
#rds_table.to_csv('rds_table.csv', index=False)

#sale_json = data_extractor.sales_json_date()
#print(sale_json)
#sale_json.to_csv('sale_json.csv', index=False)

# pdf_df = data_extractor.retrieve_pdf_data(pdf_link1) # pdf_df is a pd dataframe
# print(pdf_df.card_provider.unique())

#store_count = data_extractor.list_number_of_stores()
#print(f'The number of stores: {store_count}')
#stores_pd_data1 = data_extractor.retrieve_stores_data(store_count)
#stores_pd_data1.to_csv('stores_data.csv', index=False)


#product_df1 = data_extractor.extract_from_s3()
#product_df1.to_csv('S3products.csv', index=False)

