import pandas as pd
from spotipy import Spotify
import pandas as pd
#from connectors.connectors import init_spotify_client, get_database_engine, write_to_database
import pandas as pd
from etl_project.connectors.spotify_api import SpotifyAPI
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient
from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
from jinja2 import Environment, FileSystemLoader, Template
from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, ForeignKey, Boolean
from sqlalchemy.engine import URL, Engine

class SqlTransform:
    def __init__(self, engine: Engine, environment: Environment, table_name: str):
        self.engine = engine
        self.environment = environment
        self.table_name = table_name
        self.template = self.environment.get_template(f"{table_name}.sql")

    def create_table_as(self) -> None:
        """
        Drops the table if it exists and creates a new copy of the table using the provided select statement.
        """
        exec_sql = f"""
            drop table if exists {self.table_name};
            create table {self.table_name} as (
                {self.template.render()}
            )
        """
        self.engine.execute(exec_sql)

    def fetch_data_to_dataframe(self) -> pd.DataFrame:
        """
        Fetch data from the table and return it as a Pandas DataFrame.
        """
        query = f"SELECT * FROM {self.table_name};"
        return pd.read_sql(query, self.engine)

    def fetch_data_as_dict(self, orient='records') -> dict:
        """
        Fetch data from the table and return it as a dictionary.
        """
        df = self.fetch_data_to_dataframe()
        return df.to_dict(orient=orient)




