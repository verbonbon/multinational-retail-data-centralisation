import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect


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
    self.credentials = self.read_db_creds()
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


conn = DatabaseConnector()
yaml_data = conn.read_db_creds()
engine = conn.init_db_engine()
table_list1 = conn.list_db_tables()

print(table_list1)
