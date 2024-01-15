import requests
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    MetaData,
    Float,
)  # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.engine import URL, Engine
from sqlalchemy.dialects import postgresql
import pandas as pd
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, ForeignKey, Boolean
from sqlalchemy import DateTime
import pandas as pd
import datetime
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, ForeignKey, Boolean
from dotenv import load_dotenv
from spotify_helpers import get_global_top_40_artists
from sqlalchemy import DateTime



def init_spotify_client(client_id: str, client_secret: str) -> Spotify:
    auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    return Spotify(auth_manager=auth_manager)


def get_database_engine(
    db_user: str, db_password: str, db_server_name: str, db_database_name: str,port:str
):
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_server_name}:{port}/{db_database_name}')

    return engine


def write_to_database(engine: Engine, df: pd.DataFrame,table_name:str):

    meta = MetaData()

    # Define Artist Table
    artist_table = Table(
        'artists', meta,
        Column('artist_id', String),
        Column('name', String),
        Column('popularity', Integer),
        Column('genres', String),
        Column('followers', Integer),
        Column('spotify_url', String),
        Column('load_date', DateTime, default=datetime.datetime.utcnow),
       # schema='raw'
    )

    tracks_table = Table(
        'tracks', meta,
        Column('track_id', String),
        Column('name', String),
        Column('artist_id', String),
        Column('album_id', String),
        Column('duration_ms', Integer),
        Column('popularity', Integer),
        Column('explicit', Boolean),
        Column('preview_url', String),
        Column('track_number', Integer),
        Column('load_date', DateTime, default=datetime.datetime.utcnow),
         #schema='raw'
    )

    albums_table = Table(
        'albums', meta,
        Column('album_id', String, primary_key=True),
        Column('name', String),
        Column('artist_id', String),
        Column('release_date', String),
        Column('total_tracks', Integer),
        Column('load_date', DateTime, default=datetime.datetime.utcnow),
         #schema='raw'
        #schema?
    )

    playlist_global = Table(
        'playlist', meta,
        Column('artist_id', String),
        Column('playlist_country', Integer))
    
    print(" The meta engine", engine)
    
    meta.create_all(engine)  # creates table if it does not exist
    df.to_sql(table_name, engine, if_exists='replace', index=False)



