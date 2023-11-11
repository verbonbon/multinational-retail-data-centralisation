# Step 6:
# Create a method called clean_user_data in the DataCleaning class which will perform the cleaning of the user data.
# You will need clean the user data, look out for:
# --NULL values,
# --errors with dates,
# --incorrectly typed values and
# --rows filled with the wrong information.

# import yaml
# import psycopg2
# from sqlalchemy import create_engine
# from sqlalchemy import inspect
import database_utils as du
import data_extraction as de
import numpy as np
import pandas as pd
import re
pd.options.display.max_columns = None
pd.options.display.max_rows = None


class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, raw_df):
        """This removes null entries into a format pandas can recognize,
           parses date, removes irregular expression from text, phone numbers,
           removes invalid uuid entries, removes original index values
        """
        user_table_clean = raw_df.copy()

        # Replace NULL entries into a format recognisable by pandas in columns
        # except company, address)
        col_list = ['first_name', 'last_name', 'email_address', 'country', 'country_code', 'phone_number', 'user_uuid']
        user_table_clean[col_list] = user_table_clean[col_list].replace('NULL', np.nan)
  
        # Parse dates in columns: date_of_birth, join_date
        user_table_clean[['date_of_birth', 'join_date']] = user_table_clean[['date_of_birth', 'join_date']].apply(lambda x: pd.to_datetime(x, format='%d/%m/%Y', errors='coerce'))
        # Remove time zone
        user_table_clean['date_of_birth'] = user_table_clean['date_of_birth'].dt.tz_localize(None)
        user_table_clean['join_date'] = user_table_clean['join_date'].dt.tz_localize(None)
        
        # Remove all non-letters in the first_name, last_name columns
        user_table_clean[['first_name', 'last_name']] = user_table_clean[['first_name', 'last_name']].replace('[^a-zA-Z-]', "", regex=True)
        
        # Remove all non-letters in the address, company columns (keeping the white space between words)
        user_table_clean[['address', 'company']] = user_table_clean[['address', 'company']].replace('[^A-Za-z0-9\s]+', "", regex=True)
       
        # Remove all new lines in address
        user_table_clean['address'] = user_table_clean['address'].replace('\n', ' ')
       
        # Remove invalid email entry
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        user_table_clean.email_address = user_table_clean.email_address.apply(lambda x: x if re.match(email_pattern, str(x)) else np.nan)
       
        # Remove entries with incorrect country and country code input
        countries = ['United Kingdom', 'United States', 'Germany']
        country_codes = ['GB', 'US', 'DE']
        user_table_clean.country = user_table_clean.country.apply(lambda x: x if x in countries else np.nan)
        user_table_clean.country_code = user_table_clean.country_code.apply(lambda x: x if x in country_codes else ('GB' if 'GB' in str(x) else np.nan))
        
        # Convert country_code to category
        user_table_clean['country_code'] = user_table_clean['country_code'].astype('category')
                
        # Check if user uuid conforms with standard format and length
        user_table_clean.user_uuid = user_table_clean.user_uuid.apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)

        # Clean column with phone number
        user_table_clean.phone_number = user_table_clean.phone_number.replace('[\D]', '', regex=True).str[-10:].str.lstrip('0')

        # Drop old index column and resetting index
        user_table_clean.drop('index', axis=1, inplace=True)
        user_table_clean.dropna(inplace=True, subset=['user_uuid'])
        user_table_clean.reset_index(drop=True, inplace=True)

        return user_table_clean

    def clean_card_data(self, raw_pdf):
        """This removes null entries into a format pandas can recognize, 
           parse date, remove irregular expression from text, phone numbers, 
           remove invalid uuid entries, remove original index values 
        """
        cards_data_clean = raw_pdf.copy()

        # Check card providers are valid and corresponding number of digits in the card 
        providers = {'Diners Club / Carte Blanche': [14], 'American Express': [15], 'JCB 16 digit': [16], 'JCB 15 digit': [15], 'Maestro': range(12, 21),'Mastercard': [16], 'Discover': [16], 'VISA 19 digit': [19], 'VISA 16 digit': [16],'VISA 13 digit': [13]}         
        cards_data_clean.card_provider = cards_data_clean.card_provider.apply(lambda x: x if x in providers else np.nan)
        cards_data_clean.dropna(inplace=True, subset = ['card_provider']) # remove nan values from the dataset
        cards_data_clean.card_number = cards_data_clean.card_number.replace('[\D]', '', regex=True)        
        cards_data_clean['card_check'] = cards_data_clean.apply(lambda row: True if (len(str(row['card_number'])) in providers[row['card_provider']]) else np.nan, axis = 1)
        cards_data_clean.card_number = cards_data_clean.card_number.apply(lambda x: str(x) if len(str(x)) in range(12,20) else np.nan)

        # parse date in columns with card expiry date and confirmed date of payment
        cards_data_clean.date_payment_confirmed = cards_data_clean.date_payment_confirmed.apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d', errors='coerce')).dt.date
        cards_data_clean.expiry_date = cards_data_clean.expiry_date.apply(lambda x: pd.to_datetime(x, format='%m/%y', errors='coerce')).dt.date

        # Remove nan values, duplicates, card check column (not needed for the next stage) and resetting index
        cards_data_clean.dropna(inplace=True, subset=['card_number'])
        cards_data_clean.drop_duplicates(inplace=True)
        cards_data_clean.drop('card_check', axis=1, inplace=True)
        cards_data_clean.reset_index(drop=True, inplace=True)

        return cards_data_clean

    def clean_store_data(self, raw_store_df):
        '''
        This cleans the data retrieve from the API and returns a pandas DataFrame
        '''
        stores_clean = raw_store_df.copy()

        # Replace NULL entries into a format recognisable by pandas in columns  
        col_list = ['address', 'longitude', 'lat', 'locality', 'opening_date', 'store_type', 'latitude', 'country_code', 'continent']
        stores_clean[col_list] = stores_clean[col_list].replace('N/A', np.nan)
        stores_clean[col_list] = stores_clean[col_list].replace('None', np.nan)
        stores_clean[col_list] = stores_clean[col_list].replace('null', np.nan)
        stores_clean[col_list] = stores_clean[col_list].replace('NULL', np.nan)
        
        # Remove new lines in address
        stores_clean['address'] = stores_clean['address'].replace('\n', ' ')
        
        # Remove all non-letters in the address, company columns (keeping the white space between words)
        stores_clean[['address', 'locality']] = stores_clean[['address', 'locality']].replace('[^A-Za-z0-9\s]+', "", regex=True)

        # Parse opening date to date time format
        stores_clean.opening_date = stores_clean.opening_date.apply(lambda x: pd.to_datetime(x, format='%d-%m-%Y', errors='coerce')).dt.date

        # Check and clean continent 
        continents = ['Europe', 'America']
        stores_clean.continent = stores_clean.continent.apply(lambda x: x if x in continents else('Europe' if 'Europe' in str(x) else('America' if 'America' in str(x) else np.nan)))

        # Check and clean country code
        country_codes = ['GB', 'US', 'DE']
        stores_clean.country_code = stores_clean.country_code.apply(lambda x: x if x in country_codes else np.nan)

        # Check and clean store type 
        store_types = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        stores_clean.store_type = stores_clean.store_type.apply(lambda x: x if x in store_types else np.nan)

        # Check and clean locality 
        stores_clean.locality.replace('[\d]', np.nan, regex=True, inplace=True)

        # Check store_code against specific format 
        # (two leters, hypen, followed by 8 letter/number)
        stores_clean.store_code = stores_clean.store_code.apply(lambda x: x if re.match('^[A-Z]{2}-[A-Z0-9]{8}$', str(x)) else np.nan)

        # Check if staff numbers is an interger
        stores_clean['staff_numbers'] = stores_clean['staff_numbers'].apply(lambda x: x if isinstance(x, int) else np.nan)
        
        # Check if staff numbers is a float
        stores_clean['longitude', 'latitude'] = stores_clean['longitude', 'latitude'].apply(lambda x: x if isinstance(x, float) else np.nan)
        
        # Retain rows of data with legit staff number
        stores_clean.dropna(subset=['staff_numbers'], inplace=True)
        stores_clean = stores_clean.astype({'staff_numbers': 'int64'})

        # Drop nan values, lat column (because it has almost no data), reset index
        # note: if lat is not removed, the .drop() function later will remove all data 
        stores_clean.drop(['index', 'lat'], axis=1, inplace=True)
        stores_clean.dropna(inplace=True, subset=['store_code', 'store_type'])

        stores_clean.drop_duplicates(inplace=True)
        stores_clean.reset_index(drop=True, inplace=True)

        return stores_clean

    def convert_product_weights(self, raw_bucket_df):
        '''
        This takes the products DataFrame (from S3 bucket) as an argument,
        removes irregular entries,
        returns the products DataFrame that standardize the weight units into kg (in 1 decimal place)
        1ml was taken as 1g, as a rough estimate
        '''   
        bucket_product_clean = raw_bucket_df.copy()
        
        # Replace NULL entries into a format recognisable by pandas in weight
        bucket_product_clean['weight'] = bucket_product_clean['weight'].replace('NULL', np.nan)
        #remove the non weight entries (remove entries not starting with a number)
        bucket_product_clean['weight'] = bucket_product_clean['weight'].apply(lambda x: x if re.match('^[0-9]', str(x)) else np.nan)
        # Trying to remove the white space in 6 x 400g??
        bucket_product_clean['weight'] = bucket_product_clean['weight'].str.strip()
        
        #extract the numbers and units into two columns
        split = bucket_product_clean.weight.str.extract('([^a-zA-Z]+)([a-zA-Z]+)', expand=True)
        bucket_product_clean["weight_float"] = split[0]
        bucket_product_clean["weight_float"] = bucket_product_clean["weight_float"].astype(float)
        bucket_product_clean["weight_unit"] = split[1]
        
        unit_list = ['kg', 'g', 'ml', 'oz']
        bucket_product_clean["weight_unit"] = bucket_product_clean["weight_unit"].apply(lambda x: x if x in unit_list else np.nan)
        bucket_product_clean["weight_unit"] = bucket_product_clean["weight_unit"].astype(str)
        
        #convert the units to kg, in 1 decimal place
        bucket_product_clean.loc[bucket_product_clean['weight_unit'] == 'kg', 'weight_conversion'] = 1
        bucket_product_clean.loc[bucket_product_clean['weight_unit'] == 'g', 'weight_conversion'] = .001
        bucket_product_clean.loc[bucket_product_clean['weight_unit'] == 'ml', 'weight_conversion'] = .001
        bucket_product_clean.loc[bucket_product_clean['weight_unit'] == 'oz', 'weight_conversion'] = 0.02834952
        
        bucket_product_clean["weight_kg"] = round(bucket_product_clean["weight_float"]*bucket_product_clean["weight_conversion"], 1)
                   
        # remove columns that is no longer needed
        bucket_product_clean.drop(['weight_float', 'weight_unit', 'weight_conversion'], axis=1, inplace=True)
        bucket_product_clean.dropna(inplace=True, subset=['weight'])
        
        return bucket_product_clean

    def clean_products_data(self, bucket_product_clean):
        """
        This takes in an instance of convert_product_weights(),
        removes all irregulat entries,
        and returns a pandas DataFrame about the products
        """
        
        # Replace NULL entries into a format recognisable by pandas in columns
        col_list = ['product_name', 'product_price', 'weight', 'category', 'EAN', 'uuid', 'removed', 'product_code']
        bucket_product_clean[col_list] = bucket_product_clean[col_list].replace('NULL', np.nan)
        
        bucket_product_clean['category'] = bucket_product_clean['category'].astype('category')
        bucket_product_clean['removed'] = bucket_product_clean['removed'].astype('category')

        # Check if user uuid conforms with standard format and length
        bucket_product_clean['uuid'] = bucket_product_clean['uuid'].apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)
        
        # Check if product_code conforms with standard format and length
        bucket_product_clean['product_code'] = bucket_product_clean['product_code'].apply(lambda x: x if re.match('^[A_Z0-9]{2}-[0-9]{7}[a-z]{1}$', str(x)) else np.nan)
        
        # Remove all non-letters in the address, company columns (keeping the white space between words)
        bucket_product_clean['product_name'] = bucket_product_clean['product_name'].replace('[^A-Za-z0-9\s]+', "", regex=True)
        
        # Parse date_adde to date time format
        bucket_product_clean['date_added'] = bucket_product_clean['date_added'].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d', errors='coerce')).dt.date
                
        # Remove nan values, duplicates, card check column (not needed for the next stage) and resetting index
        bucket_product_clean.dropna(inplace=True, subset=['uuid'])
        bucket_product_clean.drop_duplicates(inplace=True)
        bucket_product_clean.reset_index(drop=True, inplace=True)

        return bucket_product_clean

    def clean_orders_data(self): 
        database_connector = du.DatabaseConnector()
        yaml_data = database_connector.read_db_creds()
        engine = database_connector.init_db_engine()
        table_list = database_connector.list_db_tables()
        print(f'Here are the names of tables: \n{table_list}')
        
        all_orders = de.read_rds_table(database_connector, 'orders_table')
        return all_orders
        
#database_connector = DatabaseConnector()
#data_extractor = DataExtractor()
#rds_table = data_extractor.read_rds_table(database_connector, 'orders_table')

datacleaning = DataCleaning()

df = datacleaning.clean_orders_data()
print(df.head(5))


#df_S3product_clean = datacleaning.convert_product_weights(dff2)
#results = datacleaning.clean_products_data(df_S3product_clean)
#df_S3product_clean.to_csv('S3product_clean.csv', index=False)
#print(df_S3product_clean.head(5))

#Print some basic info about the dataframe
#print(df_clean.describe())
#print(df_clean.country.unique())
#print(df_clean.country_code.unique())

#print(df_clean.country.isna().sum())
#print(df_clean.country_code.isna().sum())
#print(df_clean.dtypes)