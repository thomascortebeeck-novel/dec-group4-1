#from dotenv import load_dotenv
#import os
#from etl_project.assets.spotify_assets import load_data,extract_artists,transform_artists,get_artist_id,extract_top_tracks,transform_tracks,extract_albums,transform_albums,extract_global_playlist_countries,transform_playlist
import pandas as pd
import psycopg2
#from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, ForeignKey, Boolean
from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, ForeignKey, Boolean
from sqlalchemy import DateTime
import datetime





from dotenv import load_dotenv
import os
from etl_project.assets.spotify_assets import (
    extract_artists,
    transform_artists,
    load,
    extract_top_tracks,
    transform_tracks,
    extract_global_playlist_countries,
    transform_playlist,
    extract_albums,
    transform_albums
)

from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.spotify_api import SpotifyAPI
from sqlalchemy import Table, MetaData, Column, Integer, String, Float




if __name__ == "__main__":
    load_dotenv()
    CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
    CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT=os.environ.get("PORT")
    
    #sp = init_spotify_client(CLIENT_ID, CLIENT_SECRET)
    spotif_client=SpotifyAPI(CLIENT_ID,CLIENT_SECRET)

    # Connect to the PostgreSQL server (default database)
    conn = psycopg2.connect(
            host=SERVER_NAME,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            dbname="postgres",  # Connect to the default database
            port=PORT
        )
    conn.autocommit = True
    cursor = conn.cursor()

    # Check if the target database exists
    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DATABASE_NAME}'")
    exists = cursor.fetchone()
    if not exists:
        # Create the database if it doesn't exist
        cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")

    # Close the connection to the default database
    cursor.close()
    conn.close()
    
    # Load the CSV file
    file_path="etl_project/data/top_artist_from_global50.csv"

    df = pd.read_csv(file_path)

    artists_id = df['artist_id']

    #### artist ###
    
    artist_data=extract_artists(spotif_client, artists_id)


    artist_df = transform_artists(artist_data)

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )


    metadata = MetaData()
    artist_table = Table(
        "artists",
        metadata,
        Column('artist_id', String),
        Column('name', String),
        Column('popularity', Integer),
        Column('genres', String),
        Column('followers', Integer),
        Column('spotify_url', String),
        Column('load_date', DateTime, default=datetime.datetime.utcnow),
       # schema='raw'
    )



    load(
        df=artist_df,
        postgresql_client=postgresql_client,
        table=artist_table,
        metadata=metadata,
        load_method="insert",
    )


    #### tracks ###

    market=["US"]

    track_data=extract_top_tracks(spotif_client,artists_id,market)


    top_track_df = transform_tracks(track_data)


    print(top_track_df)

    tracks_table = Table(
        'tracks', metadata,
        Column('track_id', String),
        Column('name', String),
        Column('artist_id', String),
        Column('album_id', String),
        Column('duration_ms', Integer),
        Column('popularity', Integer),
        Column('explicit', Boolean),
        Column('preview_url', String),
        Column('track_number', Integer),
        Column('market', String),
        Column('load_date', DateTime, default=datetime.datetime.utcnow),
         #schema='raw'
    )


    load(
        df=top_track_df,
        postgresql_client=postgresql_client,
        table=tracks_table,
        metadata=metadata,
        load_method="insert",
    )






    

    #### albums ###
    
    # Extract albums
    album_data = extract_albums(spotif_client, artists_id)

    # Transform album data
    album_df = transform_albums(album_data)
    #print(album_df)



    albums_table = Table(
        'albums', metadata,
        Column('album_id', String),
        Column('album_name', String),
        Column('album_type', String),
        Column('artist_id', String),
        Column('release_date', String),
        Column('total_tracks', Integer),
        Column('load_date', DateTime, default=datetime.datetime.utcnow),
         #schema='raw'
        #schema?
    )

    # Load album data into the database

    load(
        df=album_df,
        postgresql_client=postgresql_client,
        table=albums_table,
        metadata=metadata,
        load_method="insert",
    )




    """
    
    playlist_dict = {
        "SE": "7jmQBEvJyGHPqKEl5UcEe9",
        "GB": "37i9dQZEVXbLnolsZ8PSNw",
        "BE": "37i9dQZEVXbJNSeeHswcKB",
        "USA":"37i9dQZEVXbLRQDuF5jeBp",
        "PT":"37i9dQZEVXbKyJS56d1pgi"
    }

    """

        
    playlist_dict = {
        "SE": "7jmQBEvJyGHPqKEl5UcEe9"
    }
    playlist_data = extract_global_playlist_countries(spotif_client,playlist_dict)
    # print(playlist_data)  

    df_playlist = transform_playlist(playlist_data)


    playlist_global = Table(
    'playlist', metadata,
    Column('artist_id', String),
    Column('playlist_country', String))


    load(
        df=df_playlist,
        postgresql_client=postgresql_client,
        table=playlist_global,
        metadata=metadata,
        load_method="insert",
    )






    """ 
    # schema needs to be added still
    
    target_engine = create_engine(target_connection_database)

    transform_environment = Environment(loader=FileSystemLoader("sql"))

    staging_albums_table = "serving_albums"
    staging_albums_sql_template = transform_environment.get_template(
        f"{staging_albums_table}.sql"
    )
    transform_test = transform(
        engine=target_engine,
        sql_template=staging_albums_sql_template,
        table_name=staging_albums_table,
    )

    print(transform_test)
    """