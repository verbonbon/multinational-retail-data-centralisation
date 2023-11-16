import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect


class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        """This reads the credentials from the yaml file
           and return a dictionary of the credentials
        """
        with open('db_creds.yaml') as credentials_yaml:
            self.credentials_dict = yaml.safe_load(credentials_yaml)
        return self.credentials_dict

    def init_db_engine(self):
        """This reads the credentials from the return of read_db_creds
           and initialise and return an sqlalchemy database engine
        """
        self.credentials_read = self.read_db_creds()
        self.credentials = self.credentials_read['credentials']
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        USER = self.credentials['RDS_USER']
        PASSWORD = self.credentials['RDS_PASSWORD']
        HOST = self.credentials['RDS_HOST']
        PORT = self.credentials['RDS_PORT']
        DATABASE = self.credentials['RDS_DATABASE']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.connect()
        return engine

    def list_db_tables(self):
        """This lists all the tables in the database,
           to show which tables the data can be extracted from
        """
        connection_engine = self.init_db_engine()
        to_inspect = inspect(connection_engine)
        table_list = to_inspect.get_table_names()
        return table_list

    def upload_to_db(self, table, table_name):
        """This uploads the cleaned table to the sales_data database
           The arguments are table and its name in quotation marks, 
           e.g., upload_to_db(user_table_clean, 'user_table_clean')
        """
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'password'
        DATABASE = 'sales_data'
        PORT = 5432
        engine_upload = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine_upload.connect()

        if table_name == 'user_data_clean':
            db_table_name = 'dim_users'
        elif table_name == 'card_data_clean':
            db_table_name = 'dim_card_details'
        elif table_name == 'store_data_clean':
            db_table_name = 'dim_store_details'
        elif table_name == 'product_data_clean':
            db_table_name = 'dim_products'
        elif table_name == 'order_data_clean':
            db_table_name = 'orders_table'
        else:
            db_table_name = 'dim_date_times'  # from sales_date_clean
        table.to_sql(db_table_name, engine_upload, if_exists='replace')
