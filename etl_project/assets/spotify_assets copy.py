import pandas as pd
from spotipy import Spotify
import pandas as pd
#from connectors.connectors import init_spotify_client, get_database_engine, write_to_database




"""
import pandas as pd
from etl_project.connectors.spotify_api import SpotifyAPI
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient
"""


#from dotenv import load_dotenv
#import os
#from etl_project.assets.spotify_assets import load_data,extract_artists,transform_artists,get_artist_id,extract_top_tracks,transform_tracks,extract_albums,transform_albums,extract_global_playlist_countries,transform_playlist
import pandas as pd
import psycopg2
#from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, ForeignKey, Boolean
#from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, ForeignKey, Boolean
from sqlalchemy import DateTime
import datetime





from dotenv import load_dotenv
import os

#from connectors.postgresql import PostgreSqlClient
from connectors.spotify_api import SpotifyAPI
from sqlalchemy import Table, MetaData, Column, Integer, String, Float


#################################### Extract ########################################################

# Example extract function for artists
def extract_artists(spotif_client: SpotifyAPI, artist_ids: list) -> list[dict]:
    artist_data = []
    for artist_id in artist_ids:
        try:
            artist = spotif_client.get_artist(artist_id)
            artist_data.append(artist)
        except Exception as e:
            print(f"An error occurred while retrieving data for artist ID {artist_id}: {e}")
    return artist_data



def extract_top_tracks_single(spotif_client: SpotifyAPI, artist_id: str,market:str) -> list[dict]:
    results = spotif_client.get_tracks(artist_id,market) ### which country is this from?
    return results['tracks']


def extract_top_tracks(spotif_client: SpotifyAPI, artist_id: str,market:str):
    top_tracks_data = []
    for id_value in artist_id:
        top_tracks_data+= extract_top_tracks_single(spotif_client, artist_id,market)
    return top_tracks_data



def extract_global_playlist_countries(spotif_client: SpotifyAPI, playlist_dict: dict):
    global_playlist = []
    for country, playlist_id in playlist_dict.items():
        top_tracks = spotif_client.get_playlist(playlist_id)
        for track_item in top_tracks['items']:
            track = track_item['track']
            global_playlist.append((track, country))  # Appending as a tuple
    return global_playlist





# we have to test the extract algums again

def extract_albums(spotify_client: SpotifyAPI, artist_ids: list) -> list[dict]:
    album_data = []
    for artist_id in artist_ids:
        url = spotify_client.get_albums(artist_id, album_type='album')  # You need to implement this method
        headers = spotify_client.get_auth_headers()  # You need to implement this method

        results = spotify_client.spotify_api_request(url, headers)
        if results is None:
            continue

        albums = results['items']
        while results.get('next'):
            results = spotify_client.spotify_api_request(results['next'], headers)
            if results is None:
                break
            albums.extend(results['items'])

        for album in albums:
            album_data.append(album)

    return album_data






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



# Load the CSV file
file_path="etl_project/data/top_artist_from_global50.csv"

df = pd.read_csv(file_path)

artists_id = df['artist_id']

#### artist ###


print(artists_id)

artist_data=extract_artists(spotif_client, artists_id)


print(artist_data)












######################################## Transform #####################################################################



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





def transform_tracks(track_data: list[dict]) -> pd.DataFrame:
    df_tracks = pd.json_normalize(track_data)
    df_selected = df_tracks[['id', 'name', 'artists', 'album.id', 'duration_ms', 'popularity', 'explicit', 'preview_url', 'track_number']]
    
    # Use .loc[] for assignments
    df_selected.loc[:, 'artist_id'] = df_selected['artists'].apply(lambda x: x[0]['id'] if x else None)
    df_selected.loc[:, 'album_id'] = df_selected['album.id']
    df_selected.rename(columns={"id": "track_id", 'album.id': 'album_id'}, inplace=True)

    return df_selected[['track_id', 'name', 'duration_ms', 'popularity', 'explicit', 'preview_url', 'track_number', 'artist_id', 'album_id']]




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

########################################## LOAD ###############################################




def load_artist(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
    ) -> None:

    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
    


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


