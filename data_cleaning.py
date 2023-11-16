from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import numpy as np
import pandas as pd
import re
pd.options.display.max_columns = None
pd.options.display.max_rows = None


class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, raw_user_df):
        """This removes null entries into a format pandas can recognise,
           parses date, removes irregular expression from text, phone numbers,
           removes invalid uuid entries, removes original index values
        """
        user_data_clean = pd.DataFrame(raw_user_df.copy())

        # Replace NULL entries into a format recognisable by pandas in columns
        # except company, address (because they have white spaces between words)
        col_list = ['first_name', 'last_name', 'date_of_birth',
                    'email_address', 'country', 'join_date',
                    'country_code', 'phone_number', 'user_uuid']
        user_data_clean[col_list] =\
            user_data_clean[col_list].replace('NULL', np.nan)

        # Parse dates in columns: date_of_birth, join_date
        # the format argument (format='%d/%m/%Y'was removed),
        # because otherwise it will return Nat
        user_data_clean[['date_of_birth', 'join_date']] =\
            user_data_clean[['date_of_birth', 'join_date']].\
            apply(lambda x: pd.to_datetime(x, errors='coerce'))

        # Remove time zone
        user_data_clean['date_of_birth'] =\
            user_data_clean['date_of_birth'].dt.tz_localize(None)
        user_data_clean['join_date'] =\
            user_data_clean['join_date'].dt.tz_localize(None)

        # Remove all non-letters in the first_name, last_name columns
        user_data_clean[['first_name', 'last_name']] =\
            user_data_clean[['first_name', 'last_name']].\
            replace('[^a-zA-Z-]', "", regex=True)

        # Remove all non-letters in the address, company columns
        # (keeping the white space between words)
        user_data_clean[['address', 'company']] =\
            user_data_clean[['address', 'company']].\
            replace('[^A-Za-z0-9\s]+', "", regex=True)

        # Remove all \n with a whitespace in address
        user_data_clean['address'] =\
            user_data_clean['address'].replace('\n', ' ')

        # Remove invalid email entry
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        user_data_clean.email_address =\
            user_data_clean.email_address.\
            apply(lambda x: x if re.match(email_pattern, str(x)) else np.nan)

        # Remove entries with incorrect country and country code input
        countries = ['United Kingdom', 'United States', 'Germany']
        country_codes = ['GB', 'US', 'DE']
        user_data_clean.country =\
            user_data_clean.country.\
            apply(lambda x: x if x in countries else np.nan)
        user_data_clean.country_code =\
            user_data_clean.country_code.\
            apply(lambda x: x if
                  x in country_codes else ('GB' if 'GB' in str(x) else np.nan))

        # Convert country_code to category
        user_data_clean['country_code'] =\
            user_data_clean['country_code'].astype('category')

        # Check if user uuid conforms with standard format and length
        user_data_clean.user_uuid =\
            user_data_clean.user_uuid.\
            apply(lambda x: x if
                  re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)

        # Clean column with phone number
        user_data_clean.phone_number =\
            user_data_clean.phone_number.\
            replace('[\D]', '', regex=True).str[-10:].str.lstrip('0')

        # Drop old index column and resetting index
        user_data_clean.drop('index', axis=1, inplace=True)
        user_data_clean.dropna(inplace=True, subset=['user_uuid'])
        user_data_clean.reset_index(drop=True, inplace=True)

        return user_data_clean

    def clean_card_data(self, raw_card):
        """This removes null entries into a format pandas can recognise,
           parse date, remove irregular expression from text, phone numbers,
           remove invalid uuid entries, remove original index values
        """
        card_data_clean = raw_card.copy()

        # Check card providers are valid and 
        # corresponding number of digits in the card
        providers = {'Diners Club / Carte Blanche': [14],
                     'American Express': [15],
                     'JCB 16 digit': [16],
                     'JCB 15 digit': [15],
                     'Maestro': range(12, 21),
                     'Mastercard': [16],
                     'Discover': [16],
                     'VISA 19 digit': [19],
                     'VISA 16 digit': [16],
                     'VISA 13 digit': [13]}
        card_data_clean.card_provider = card_data_clean.card_provider.\
            apply(lambda x: x if x in providers else np.nan)
        card_data_clean.dropna(inplace=True,
                               subset=['card_provider'])
        card_data_clean.card_number =\
            card_data_clean.card_number.replace('[\D]', '', regex=True)
        card_data_clean['card_check'] = card_data_clean.\
            apply(lambda row: True if (len(str(row['card_number'])) in
                  providers[row['card_provider']]) else np.nan, axis=1)
        card_data_clean.card_number =\
            card_data_clean.card_number.apply(lambda x: str(x) if len(str(x))
                                              in range(12, 20) else np.nan)

        # parse date in columns with card expiry date
        # and confirmed date of payment
        card_data_clean.date_payment_confirmed =\
            card_data_clean.date_payment_confirmed.\
            apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d',
                                           errors='coerce')).dt.date
        card_data_clean.expiry_date =\
            card_data_clean.expiry_date.\
            apply(lambda x: pd.to_datetime(x,
                                           format='%m/%y', 
                                           errors='coerce')).dt.date

        # Remove nan values, duplicates, card check column
        # (not needed for the next stage) and resetting index
        card_data_clean.dropna(inplace=True, subset=['card_number'])
        card_data_clean.drop_duplicates(inplace=True)
        card_data_clean.drop('card_check', axis=1, inplace=True)
        card_data_clean.reset_index(drop=True, inplace=True)

        return card_data_clean

    def clean_store_data(self, raw_store_df):
        """This cleans the data retrieve from the API
           and returns a pandas DataFrame
        """
        store_data_clean = raw_store_df.copy()

        # Replace NULL entries into a format recognisable by pandas in columns
        col_list = ['address', 'longitude', 'lat', 'locality', 'opening_date',
                    'store_type', 'latitude', 'country_code', 'continent']
        store_data_clean[col_list] =\
            store_data_clean[col_list].replace('N/A', np.nan)
        store_data_clean[col_list] =\
            store_data_clean[col_list].replace('None', np.nan)
        store_data_clean[col_list] =\
            store_data_clean[col_list].replace('null', np.nan)
        store_data_clean[col_list] =\
            store_data_clean[col_list].replace('NULL', np.nan)

        # Remove new lines in address
        store_data_clean['address'] =\
            store_data_clean['address'].replace('\n', ' ')

        # Remove all non-letters in the address, company columns
        # (keeping the white space between words)
        store_data_clean[['address', 'locality']] =\
            store_data_clean[['address', 'locality']].\
            replace('[^A-Za-z0-9\s]+', "", regex=True)

        # Parse opening date to date time format
        # again, needed to remove: format='%d-%m-%Y'
        # to avoid resulting in NaT
        store_data_clean.opening_date =\
            store_data_clean.opening_date.\
            apply(lambda x: pd.to_datetime(x,
                                           errors='coerce')).dt.date

        # Check and clean continent 
        continents = ['Europe', 'America']
        store_data_clean.continent =\
            store_data_clean.continent.\
            apply(lambda x: x if x in continents else (
                'Europe' if 'Europe' in str(x) else ('America' if 'America' 
                                                     in str(x) else np.nan)))

        # Check and clean country code
        country_codes = ['GB', 'US', 'DE']
        store_data_clean.country_code = store_data_clean.country_code.\
            apply(lambda x: x if x in country_codes else np.nan)

        # Check and clean store type
        store_types = ['Local', 'Super Store', 'Mall Kiosk',
                       'Outlet', 'Web Portal']
        store_data_clean.store_type = store_data_clean.store_type.\
            apply(lambda x: x if x in store_types else np.nan)

        # Check and clean locality 
        store_data_clean.locality.replace('[\d]',
                                          np.nan, regex=True, inplace=True)

        # Check store_code against specific format
        # (two leters, hypen, followed by 8 letter/number)
        store_data_clean.store_code = store_data_clean.store_code.\
            apply(lambda x: x if
                  re.match('^[A-Z]{2}-[A-Z0-9]{8}$', str(x)) else np.nan)

        # Drop nan values, lat column (because it has almost no data),
        # reset index
        store_data_clean.drop(['index', 'lat'], axis=1, inplace=True)
        store_data_clean.dropna(inplace=True, 
                                subset=['store_code', 'store_type'])
        store_data_clean.drop_duplicates(inplace=True)
        store_data_clean.reset_index(drop=True, inplace=True)

        return store_data_clean

    def convert_product_weights(self, raw_product_df):
        """This takes the products DataFrame (from S3 bucket) as an argument,
        removes irregular entries,
        returns the products DataFrame that standardize
        the weight units into kg (in 1 decimal place)
        1ml was taken as 1g, as a rough estimate
        """
        product_clean = raw_product_df.copy()

        # Replace NULL entries into a format recognisable by pandas in weight
        product_clean['weight'] =\
            product_clean['weight'].replace('NULL', np.nan)

        # Remove the non weight entries
        # (remove entries not starting with a number)
        product_clean['weight'] =\
            product_clean['weight'].apply(lambda x: x if
                                          re.match('^[0-9]', str(x))
                                          else np.nan)

        # Trying to remove the white space in 6 x 400g, but didn't work
        product_clean['weight'] =\
            product_clean['weight'].str.strip()

        # Extract the numbers and units into two columns
        # such that the numbers can be manipulated later
        split = product_clean.weight.str.\
            extract('([^a-zA-Z]+)([a-zA-Z]+)', expand=True)
        product_clean["weight_float"] = split[0]
        product_clean["weight_float"] =\
            product_clean["weight_float"].astype(float)
        product_clean["weight_unit"] = split[1]

        unit_list = ['kg', 'g', 'ml', 'oz']
        product_clean["weight_unit"] =\
            product_clean["weight_unit"].\
            apply(lambda x: x if x in unit_list else np.nan)
        product_clean["weight_unit"] =\
            product_clean["weight_unit"].astype(str)

        # Convert the units to kg, in 1 decimal place
        product_clean.loc[product_clean['weight_unit'] ==
                          'kg', 'weight_conversion'] = 1
        product_clean.loc[product_clean['weight_unit'] ==
                          'g', 'weight_conversion'] = .001
        product_clean.loc[product_clean['weight_unit'] ==
                          'ml', 'weight_conversion'] = .001
        product_clean.loc[product_clean['weight_unit'] ==
                          'oz', 'weight_conversion'] = 0.02834952

        product_clean["weight_kg"] =\
            round(product_clean["weight_float"] *
                  product_clean["weight_conversion"], 1)

        # Remove columns no longer needed
        product_clean.drop(['weight_float',
                            'weight_unit', 'weight_conversion'],
                           axis=1, inplace=True)
        product_clean.dropna(inplace=True, subset=['weight'])

        return product_clean

    def clean_products_data(self, product_clean):
        """This takes in an instance of convert_product_weights(),
           removes all irregulat entries,
           and returns a pandas DataFrame about the products
        """
        product_data_clean = product_clean.copy()

        # Replace NULL entries into a format recognisable by pandas in columns
        col_list = ['product_name', 'product_price', 'weight', 'category',
                    'EAN', 'uuid', 'removed', 'product_code']
        product_data_clean[col_list] =\
            product_data_clean[col_list].replace('NULL', np.nan)

        product_data_clean['category'] =\
            product_data_clean['category'].astype('category')
        product_data_clean['removed'] =\
            product_data_clean['removed'].astype('category')

        # Check if user uuid conforms with standard format and length
        product_data_clean['uuid'] =\
            product_data_clean['uuid'].\
            apply(lambda x: x if
                  re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)

        # Check if product_code conforms with standard format and length
        # code stucture is, e.g., : R7-3126933h
        # the second part of the codes can be as short as 7, 
        # but if I used [0-9a-zA-Z]{7, 8}, it would remove all product codes
        # product_data_clean['product_code'] = product_data_clean['product_code'].\
        #    apply(lambda x: x if re.match('^[A-Za-z0-9]{2}-[0-9a-zA-Z]{8}$', str(x)) else np.nan)

        # Remove all non-letters in the address, company columns
        # (keeping the white space between words)
        product_data_clean['product_name'] =\
            product_data_clean['product_name'].\
            replace('[^A-Za-z0-9\s]+', "", regex=True)

        # Parse date_adde to date time format
        product_data_clean['date_added'] =\
            product_data_clean['date_added'].\
            apply(lambda x: pd.to_datetime(x,
                                           format='%Y-%m-%d',
                                           errors='coerce')).dt.date

        # Remove nan values, duplicates,
        # card check column (not needed for the next stage)
        # resetting index
        product_data_clean.dropna(inplace=True, subset=['uuid'])
        product_data_clean.drop_duplicates(inplace=True)
        product_data_clean.reset_index(drop=True, inplace=True)

        return product_data_clean

    def clean_orders_data(self):
        """This drops the five almost emtpy columns:
           first_name, last_name, 1, level_0, index
           Checks and validates the card number, product_quantity
           user_uuid, and product_code
        """
        database_connector = DatabaseConnector()
        table_list = database_connector.list_db_tables()
        print(f'Here are the names of tables: \n{table_list}')

        data_extractor = DataExtractor()
        order_data_clean = data_extractor.read_rds_table(database_connector,
                                                         'orders_table')

        order_data_clean = order_data_clean.\
            drop(['first_name', 'last_name', '1', 'level_0', 'index'], axis=1)

        # Check irregular entries containing alphabetical characters
        # in card_number column;
        # (credit cards usually have at least 16 digits)
        # so not sure if the shorter ones are valid
        order_data_clean['card_number'] =\
            order_data_clean['card_number'].replace('[\D]', '', regex=True)      

        # Convert product quantity column to numeric data type
        order_data_clean['product_quantity'] =\
            order_data_clean['product_quantity'].\
            apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('int64')

        # Validate date_uuid and user_uuid column format
        order_data_clean['user_uuid'] =\
            order_data_clean['user_uuid'].\
            apply(lambda x: x if
                  re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x))
                  else np.nan)

        order_data_clean['date_uuid'] =\
            order_data_clean['date_uuid'].\
            apply(lambda x: x if
                  re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x))
                  else np.nan)

        # Drop nan rows, reset index
        order_data_clean.dropna(inplace=True, subset=['date_uuid'])
        order_data_clean.reset_index(drop=True, inplace=True)

        return order_data_clean

    def clean_sales_date(self, raw_date_json):
        """This cleans the json file with sales date
        and returns a dataframe
        """
        sales_date_clean = raw_date_json.copy()

        # Denote NULL entries as format recognisable by pandas / numpy
        sales_date_clean = raw_date_json.replace('NULL', np.nan)

        # Convert timestamp column to datatime.time format
        sales_date_clean['timestamp'] = sales_date_clean['timestamp'].\
            apply(lambda x: pd.to_datetime(x,
                                           format='%H:%M:%S',
                                           errors='coerce')).dt.time

        # Clean up months column by removing invalid entries
        sales_date_clean['month'] = sales_date_clean['month'].\
            apply(lambda x: x if pd.to_numeric(x, errors='coerce') in
                  [*range(1, 13)] else np.nan)

        # Clean up days column by removing invalid entries
        sales_date_clean['day'] = sales_date_clean['day'].\
            apply(lambda x: x if pd.to_numeric(x, errors='coerce') in
                  [*range(1, 32)] else np.nan)

        # Clean up years column by removing invalid entries
        sales_date_clean['year'] = sales_date_clean['year'].\
            apply(lambda x: x if pd.to_numeric(x, errors='coerce') in
                  [*range(1980, 2024)] else np.nan)

        # Clean up user uuid column by removing entries of invalid length
        sales_date_clean['date_uuid'] = sales_date_clean['date_uuid'].\
            apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)

        # Clean up periods column by removing invalid entries
        period = ['Morning', 'Midday', 'Evening', 'Late_Hours']
        sales_date_clean['time_period'] = sales_date_clean['time_period'].\
            apply(lambda x: x if x in period else np.nan)

        # Remove nan values, duplicates and reset index
        sales_date_clean.dropna(inplace=True, subset=['date_uuid'])
        sales_date_clean.drop_duplicates(inplace=True)
        sales_date_clean.reset_index(drop=True, inplace=True)

        return sales_date_clean

    # Testing within this file

    def test_code():
        """This tests all the methods in the class
        and returns the first 5 lines of each cleaned dataframe
        """
        database_connector = DatabaseConnector()
        data_extractor = DataExtractor()
        data_cleaning = DataCleaning()

        # clean user data
        table_name = 'legacy_users'
        user_data = data_extractor.read_rds_table(database_connector,
                                                  table_name)
        user_data_clean = data_cleaning.clean_user_data(user_data)
        print(f'Here is first 5 lines of the clean user data:\
              \n {user_data_clean.head(5)}')

        # clean card data
        pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        card_data = data_extractor.retrieve_pdf_data(pdf_link)
        card_data_clean = data_cleaning.clean_card_data(card_data)
        print(f'Here is first 5 lines of the clean card data:\
              \n {card_data_clean.head(5)}')

        # clean store data
        number_of_stores = data_extractor.list_number_of_stores()
        stores_data = data_extractor.retrieve_stores_data(number_of_stores)
        store_data_clean = data_cleaning.clean_store_data(stores_data)
        print(f'Here is first 5 lines of the clean stores data:\
              \n {store_data_clean.head(5)}')

        # clean product data
        product_data = data_extractor.extract_from_s3()
        product_weight_converted = data_cleaning.\
            convert_product_weights(product_data)
        product_data_clean = data_cleaning.\
            clean_products_data(product_weight_converted)
        print(f'Here is first 5 lines of the clean product data:\
              \n {product_data_clean.head(5)}')

        # clean order data
        order_data_clean = data_cleaning.clean_orders_data()
        print(f'Here is first 5 lines of the clean order data:\
              \n {order_data_clean.head(5)}')

        # clean date data
        sales_date = data_extractor.sales_json_date()
        sales_date_clean = data_cleaning.clean_sales_date(sales_date)
        print(f'Here is first 5 lines of the clean sales date:\
              \n {sales_date_clean.head(5)}')


if __name__ == '__main__':
    testing = DataCleaning.test_code()
    print(testing)
