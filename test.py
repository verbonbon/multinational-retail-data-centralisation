
import numpy as np
import pandas as pd
import re
pd.options.display.max_columns = None
pd.options.display.max_rows = None

df1 = pd.read_csv("df.csv")


def clean_user_data(df):
        """This removes null entries into a format pandas can recognize,
           parses date, removes irregular expression from text, phone numbers,
           removes invalid uuid entries, removes original index values
        """
        user_table_clean = pd.DataFrame(df.copy())

        # Replace NULL entries into a format recognisable by pandas in columns
        # except company, address
        col_list = ['first_name', 'last_name', 'date_of_birth',
                    'email_address', 'country', 'join_date',
                    'country_code', 'phone_number', 'user_uuid']
        user_table_clean[col_list] =\
            user_table_clean[col_list].replace('NULL', np.nan)

        # Parse dates in columns: date_of_birth, join_date
        user_table_clean['date_of_birth'] =\
            pd.to_datetime(user_table_clean['date_of_birth'],
                           format='%d/%m/%Y', errors='coerce').dt.date

        user_table_clean['join_date'] =\
            pd.to_datetime(user_table_clean['join_date'],
                           format='%d/%m/%Y', errors='coerce').dt.date

        # Remove all non-letters in the first_name, last_name columns
        user_table_clean[['first_name', 'last_name']] =\
            user_table_clean[['first_name', 'last_name']].\
            replace('[^a-zA-Z-]', "", regex=True)

        # Remove all non-letters in the address, company columns
        # (keeping the white space between words)
        user_table_clean[['address', 'company']] =\
            user_table_clean[['address', 'company']].\
            replace('[^A-Za-z0-9\s]+', "", regex=True)

        # Remove all new lines in address
        user_table_clean['address'] =\
            user_table_clean['address'].replace('\n', ' ')

        # Remove invalid email entry
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        user_table_clean.email_address =\
            user_table_clean.email_address.\
            apply(lambda x: x if re.match(email_pattern, str(x)) else np.nan)

        # Remove entries with incorrect country and country code input
        countries = ['United Kingdom', 'United States', 'Germany']
        country_codes = ['GB', 'US', 'DE']
        user_table_clean.country =\
            user_table_clean.country.\
                apply(lambda x: x if x in countries else np.nan)
        user_table_clean.country_code =\
            user_table_clean.country_code.\
            apply(lambda x: x if
                  x in country_codes else ('GB' if 'GB' in str(x) else np.nan))

        # Convert country_code to category
        user_table_clean['country_code'] =\
            user_table_clean['country_code'].astype('category')

        # Check if user uuid conforms with standard format and length
        user_table_clean.user_uuid =\
            user_table_clean.user_uuid.\
            apply(lambda x: x if
                  re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)

        # Clean column with phone number
        user_table_clean.phone_number =\
            user_table_clean.phone_number.\
            replace('[\D]', '', regex=True).str[-10:].str.lstrip('0')

        # Drop old index column and resetting index
        user_table_clean.drop('index', axis=1, inplace=True)
        user_table_clean.dropna(inplace=True, subset=['user_uuid'])
        user_table_clean.reset_index(drop=True, inplace=True)

        return (user_table_clean.head(5))


clean = clean_user_data(df1)
print(clean)
print(clean.describe())
