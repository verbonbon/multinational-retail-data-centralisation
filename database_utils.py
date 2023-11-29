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
        #TODO: This 'db_creds.yaml' could be replaced with a parameter instead. 
        # e.g. def read_db_creds(self, config_file_name)
        # with open(config_file_name) as credentials_yaml 
        # This allows you to pass in any config file name and read it in as a .yaml file. 
        with open('db_creds.yaml') as credentials_yaml:
            self.credentials_dict = yaml.safe_load(credentials_yaml)
        return self.credentials_dict

    def init_db_engine(self):
        """This reads the credentials from the return of read_db_creds
           and initialise and return an sqlalchemy database engine
        """
        self.credentials_read = self.read_db_creds()
        self.credentials = self.credentials_read['credentials']
        #TODO: lines 29-30 can be placed inside the db_creds.yaml file to avoid hard-coding them in this method
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
        #FIXME: lines 51 - 57 should be placed inside of a .yaml file this is a big security risk. 
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'xxxx'
        DATABASE = 'sales_data'
        PORT = 5432
        engine_upload = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine_upload.connect()
        #TODO: this if/elif statement is overengineered and will be harder to maintain in the long run. 
        # I do understand why it's used here to be specific with what table is uploaded,
        # but you can already specify the table name in df.to_sql() so I find that the if/elifs are redundant here. 
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
