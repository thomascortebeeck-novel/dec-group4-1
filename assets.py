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

import pandas as pd
from datetime import datetime, timezone
from datetime import datetime, timezone, timedelta
from connectors import init_spotify_client, get_database_engine, write_to_database


# Example extract function for artists
def extract_artists(sp: Spotify, artist_ids: list) -> list[dict]:
    artist_data = []
    for artist_id in artist_ids:
        try:
            artist = sp.artist(artist_id)
            artist_data.append(artist)
        except Exception as e:
            print(f"An error occurred while retrieving data for artist ID {artist_id}: {e}")
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



def extract_global_playlist_countries(sp, playlist_dict):
    global_playlist = []
    for country, playlist_id in playlist_dict.items():
        top_tracks = sp.playlist_tracks(playlist_id)
        for track_item in top_tracks['items']:
            track = track_item['track']
            global_playlist.append((track, country))  # Appending as a tuple
    return global_playlist


def transform_playlist(playlist_data):
    # Initialize an empty list to hold artist IDs and countries
    artist_data = []

    # Loop through each track in the playlist data
    for track, country in playlist_data:
        # Each track can have multiple artists
        for artist in track['artists']:
            # Extract the artist ID and append it along with the country to the list
            artist_data.append((artist['id'], country))

    # Convert the list of tuples into a DataFrame
    df_artists = pd.DataFrame(artist_data, columns=['artist_id', 'playlist_country'])

    return df_artists


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
def load_data(df: pd.DataFrame, table_name: str,
        db_user: str,
        db_password: str,
        db_server_name: str,
        db_database_name: str,
        port:str
    ):

    engine = get_database_engine(
        db_user=db_user,
        db_password=db_password,
        db_server_name=db_server_name,
        db_database_name=db_database_name,
        port=port
    )
    write_to_database(engine,df, table_name)


