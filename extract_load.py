import os
import pandas as pd
import psycopg2
import json
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

# Example extract function for artists
def extract_artists(sp: Spotify, artists: list) -> list[dict]:
    artist_data = []
    for artist_name in artists:
        results = sp.search(q=f'artist:{artist_name}', type='artist', limit=1)
        artist_items = results['artists']['items']
        if artist_items:
            artist_data.append(artist_items[0])
    return artist_data


def extract_top_tracks_single(sp: Spotify, artist_id: str) -> list[dict]:
    results = sp.artist_top_tracks(artist_id) ### which country is this from?
    return results['tracks']


def extract_top_tracks(sp, artist_data):
    top_tracks_data = []
    artist_id= get_artist_id(artist_data)
    for id_value in artist_id:
        top_tracks_data+= extract_top_tracks_single(sp, id_value)
    return top_tracks_data


def get_artist_id(artist_data):
    return  [artist['id'] for artist in artist_data]


def transform_tracks(track_data: list[dict]) -> pd.DataFrame:
    df_tracks = pd.json_normalize(track_data)
    df_selected = df_tracks[['id', 'name', 'artists', 'album.id', 'duration_ms', 'popularity', 'explicit', 'preview_url', 'track_number']]
    
    # Use .loc[] for assignments
    df_selected.loc[:, 'artist_id'] = df_selected['artists'].apply(lambda x: x[0]['id'] if x else None)
    df_selected.loc[:, 'album_id'] = df_selected['album.id']
    df_selected.rename(columns={"id": "track_id", 'album.id': 'album_id'}, inplace=True)

    return df_selected[['track_id', 'name', 'duration_ms', 'popularity', 'explicit', 'preview_url', 'track_number', 'artist_id', 'album_id']]

# extract functions for tracks, albums, playlists, and user profiles based on artist names
def extract_albums(sp: Spotify, artist_ids: list) -> list[dict]:
    album_data = []
    for artist_id in artist_ids:
        # Fetch albums for each artist
        results = sp.artist_albums(artist_id, album_type='album')
        albums = results['items']
        while results['next']:
            results = sp.next(results)
            albums.extend(results['items'])

        for album in albums:
            album_data.append(album)
    return album_data


def transform_albums(album_data: list[dict]) -> pd.DataFrame:
    df_albums = pd.json_normalize(album_data)
    df_selected = df_albums[['id', 'name', 'artists', 'release_date', 'total_tracks']]
    
    # Transforming and renaming the columns
    df_selected.loc[:, 'artist_id'] = df_selected['artists'].apply(lambda x: x[0]['id'] if x else None)
    df_selected.rename(columns={'id': 'album_id', 'release_date': 'release_date', 'total_tracks': 'total_tracks'}, inplace=True)

    return df_selected[['album_id', 'name', 'release_date', 'total_tracks', 'artist_id']]


def transform_artists(artist_data: list[dict]) -> pd.DataFrame:
    df_artists = pd.json_normalize(artist_data)
    df_selected = df_artists[["id",'name', 'popularity', 'genres', 'followers.total', 'external_urls.spotify']]
    df_selected.rename(columns={"id":"artist_id",'followers.total': 'followers', 'external_urls.spotify': 'spotify_url'}, inplace=True)
    return df_selected


# Similar transform functions for tracks, albums, playlists, and user profiles
def load_data(df: pd.DataFrame, table_name: str, engine):
    print("here")
    print(df)
    df.to_sql(table_name, engine, if_exists='replace', index=False)

def main():
    load_dotenv()
    CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
    CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")

    sp = init_spotify_client(CLIENT_ID, CLIENT_SECRET)

    # Connect to the PostgreSQL server (default database)
    conn = psycopg2.connect(
        host=SERVER_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        dbname="postgres"  # Connect to the default database
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
    
    # Connect to spotify database
    engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{SERVER_NAME}/{DATABASE_NAME}')
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
    # Create tables
    meta.create_all(engine)

    # Example ETL process for artists
    # artists = ["Adele", "Ed Sheeran", "Taylor Swift"]
    
    #### artist ###

    # Load the CSV file
    file_path = 'top_40_artists.csv'
    df = pd.read_csv(file_path)

    # Extract the 'Artist Name' column
    artists = df['Artist Name']
    
    #### 

    artist_data = extract_artists(sp, artists)

    artist_df = transform_artists(artist_data)

    load_data(artist_df, 'artists', engine)

    #artist_ids=get_artist_id(artist_data)

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


    # Example ETL process for artists
    # artists = ["Adele", "Ed Sheeran", "Taylor Swift"]

    #### artist ###
    
    artist_data = extract_artists(sp, artists)

    artist_df = transform_artists(artist_data)

    load_data(artist_df, 'artists', engine)

    artist_ids=get_artist_id(artist_data)

    #### tracks ###

    track_data=extract_top_tracks(sp,artist_data)

    top_track_df = transform_tracks(track_data)

    load_data(top_track_df, 'tracks', engine)

    #### albums ###
    
    # Extract albums
    album_data = extract_albums(sp, artist_ids)

    # Transform album data
    album_df = transform_albums(album_data)

    # Load album data into the database
    load_data(album_df, 'albums', engine)
    # Similar ETL processes for tracks, albums, playlists, user profiles

if __name__ == "__main__":
    main()










## cam we make incremental from the api