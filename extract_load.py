import os
import pandas as pd
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, ForeignKey, Boolean
from dotenv import load_dotenv

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
    results = sp.artist_top_tracks(artist_id)
    return results['tracks']
 

def extract_top_tracks(sp, artist_data):
    top_tracks_data = []
    for artist in artist_data:
        artist_id = artist['id']
        top_tracks_data+= extract_top_tracks_single(sp, artist_id)
    return top_tracks_data


def transform_tracks(track_data: list[dict]) -> pd.DataFrame:
    df_tracks = pd.json_normalize(track_data)
    df_selected = df_tracks[['id', 'name', 'artists', 'album.id', 'duration_ms', 'popularity', 'explicit', 'preview_url', 'track_number']]
    
    # Use .loc[] for assignments
    df_selected.loc[:, 'artist_id'] = df_selected['artists'].apply(lambda x: x[0]['id'] if x else None)
    df_selected.loc[:, 'album_id'] = df_selected['album.id']
    df_selected.rename(columns={"id": "track_id", 'album.id': 'album_id'}, inplace=True)

    return df_selected[['track_id', 'name', 'artist_id', 'album_id', 'duration_ms', 'popularity', 'explicit', 'preview_url', 'track_number']]




# extract functions for tracks, albums, playlists, and user profiles based on artist names


# Example transform function for artists
def transform_artists(artist_data: list[dict]) -> pd.DataFrame:
    df_artists = pd.json_normalize(artist_data)
    df_selected = df_artists[["id",'name', 'popularity', 'genres', 'followers.total', 'external_urls.spotify']]
    df_selected.rename(columns={"id":"artist_id",'followers.total': 'followers', 'external_urls.spotify': 'spotify_url'}, inplace=True)
    return df_selected

# Similar transform functions for tracks, albums, playlists, and user profiles

def load_data(df: pd.DataFrame, table_name: str, engine):
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

    engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{SERVER_NAME}/{DATABASE_NAME}')
    meta = MetaData()

    # Define Artist Table
    artist_table = Table(
        'artists', meta,
        Column('artist_id', String, primary_key=True), # id doesn't exist ?
        Column('name', String),
        Column('popularity', Integer),
        Column('genres', String),
        Column('followers', Integer),
        Column('spotify_url', String)
    )

    tracks_table = Table(
        'tracks', meta,
        Column('track_id', String, primary_key=True),
        Column('name', String),
        Column('artist_id', String, ForeignKey('artists.artist_id')),  # Assuming artist table has 'artist_id'
        Column('album_id', String, ForeignKey('albums.album_id')),    # Assuming album table has 'album_id'
        Column('duration_ms', Integer),
        Column('popularity', Integer),
        Column('explicit', Boolean),
        Column('preview_url', String),
        Column('track_number', Integer)
    )

    albums_table = Table(
        'albums', meta,
        Column('album_id', String, primary_key=True),
        Column('name', String),
        Column('artist_id', String, ForeignKey('artists.artist_id')),  # Link to artists
        Column('release_date', String),
        Column('total_tracks', Integer)
    )

    # Create tables
    meta.create_all(engine)

    # Example ETL process for artists
    artists = ["Adele", "Ed Sheeran", "Taylor Swift"]

    artist_data = extract_artists(sp, artists)

    track_data=extract_top_tracks(sp,artist_data)

    artist_df = transform_artists(artist_data)

    top_track_df = transform_tracks(track_data)

    load_data(artist_df, 'artists', engine)


    load_data(top_track_df, 'tracks', engine)

    

    # Similar ETL processes for tracks, albums, playlists, user profiles

if __name__ == "__main__":
    main()

