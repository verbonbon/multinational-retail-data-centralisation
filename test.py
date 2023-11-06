import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd


class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        """This reads the credentials from the yaml file
           and return a dictionary of the credentials.
        """
        with open('db_creds.yaml') as credentials_yaml:
            self.credentials_dict = yaml.safe_load(credentials_yaml)
        return self.credentials_dict

    def init_db_engine(self):
        """This reads the credentials from the return of read_db_creds
           and initialise and return an sqlalchemy database engine.
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


class DataExtractor:
    def __init__(self):
        pass


    def read_rds_table(self, DatabaseConnector, table_name):
        """ This takes in an instance of the DatabaseConnector class 
            and the table name as an argument and returns a pandas DataFrame containing user data
        """
        engine = DatabaseConnector.init_db_engine('db_creds.yaml')
        query = (f'SELECT * FROM {table_name}')
        df = pd.read_sql_query(query, engine)
        print(df)
        return df
   
DataExtractor("legacy_users")
