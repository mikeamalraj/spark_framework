from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os
import pandas as pd
from src.root import ENV_CONF_PATH
from src.utils.fileutil import read_json_file
from src.utils.encryption import decrypt_message


def initialize_db(env):
    config = read_json_file(ENV_CONF_PATH)[env.upper()]
    pwd = decrypt_message(os.getenv("SECRETKEY"), os.getenv("CONFDBPWD"))
    connection_url = URL.create(
        config['Driver'],
        username=config['User'],
        password=pwd,
        host=config['Host'],
        port=config['Port'],
        database=config['Database']

    )
    sql_engine = create_engine(connection_url)
    return sql_engine


class TalkToDb:

    def __init__(self, env: str):
        self.env = env
        self.conn = initialize_db(env)

    def read_from_db(self, query: str, params=None, parse_dates=None, columns=None):
        return pd.read_sql(sql=query, con=self.conn, params=params, parse_dates=parse_dates, columns=columns)

    def write_to_db(self, df: pd.DataFrame, table_name, replace=False):
        try:
            if replace:
                df.to_sql(table_name, con=self.conn, if_exists='replace', index=False)
            else:
                df.to_sql(table_name, con=self.conn, if_exists='append', index=False)
        except Exception as e:
            print(e)
            raise Exception("Exception occurred while saving data")

    def execute_query(self, query: str):
        with self.conn.connect() as conn:
            result = conn.execute(query)
            return result

    def execute_query_with_param(self, query: str, param=None):
        with self.conn.connect() as conn:
            result = conn.exec_driver_sql(statement=query, parameters=param)
            return result
