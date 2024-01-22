import pandas as pd
from spotipy import Spotify
import pandas as pd
#from connectors.connectors import init_spotify_client, get_database_engine, write_to_database





import pandas as pd
from etl_project.connectors.spotify_api import SpotifyAPI
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient


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



def extract_top_tracks_single(spotif_client: SpotifyAPI, artist_id: list ,market:str = "US") -> list[dict]:
    results = spotif_client.get_tracks(artist_id,market) ### which country is this from?
    return results["tracks"]



def extract_top_tracks(spotify_client: SpotifyAPI, artist_ids: list[str], markets: list[str]) -> list[tuple[dict, str]]:
    top_tracks_data = []
    for market in markets:
        for artist_id in artist_ids:
            tracks = extract_top_tracks_single(spotify_client, artist_id, market)
            for track in tracks:
                top_tracks_data.append((track, market))
    return top_tracks_data



def extract_global_playlist_countries(spotif_client: SpotifyAPI, playlist_dict: dict):
    global_playlist = []
    for country, playlist_id in playlist_dict.items():
        top_tracks = spotif_client.get_playlist(playlist_id)
        for track_item in  top_tracks['tracks']['items']:
            track = track_item['track']
            global_playlist.append((track, country))  # Appending as a tuple
    return global_playlist




# Example extract function for artists
def extract_albums(spotif_client: SpotifyAPI, artist_ids: list) -> list[dict]:
    albums_data = []
    for artist_id in artist_ids:
        try:
            albums = spotif_client.get_albums(artist_id)
            albums_data.append(albums)
        except Exception as e:
            print(f"An error occurred while retrieving data for artist ID {artist_id}: {e}")
    return albums_data


























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





def transform_tracks(track_data: list[tuple[dict, str]]) -> pd.DataFrame:
    # Normalize the track data to flatten nested structures
    df_tracks = pd.DataFrame()

    for track, market in track_data:
        df_track = pd.json_normalize(track, sep='_')
        df_track['market'] = market
        df_tracks = pd.concat([df_tracks, df_track], ignore_index=True)

    # Extract artist ID from the nested artists array
    df_tracks['artist_id'] = df_tracks['artists'].apply(lambda x: x[0]['id'] if x else None)

    # Extract album ID from the nested album object
    df_tracks['album_id'] = df_tracks['album_id']

    # Select and rename columns
    columns_to_select = {
        'id': 'track_id',
        'name': 'name',
        'duration_ms': 'duration_ms',
        'popularity': 'popularity',
        'explicit': 'explicit',
        'preview_url': 'preview_url',
        'track_number': 'track_number',
        'artist_id': 'artist_id',
        'album_id': 'album_id',
        'market': 'market'
    }
    df_selected = df_tracks[[col for col in columns_to_select.keys() if col in df_tracks.columns]]
    df_selected.rename(columns=columns_to_select, inplace=True)

    return df_selected







def transform_albums(album_data_list: list) -> pd.DataFrame:
    transformed_data = []

    # Iterate over each artist's album data in the list
    for album_data in album_data_list:
        # Iterate over the 'items' in each artist's album data
        for album in album_data['items']:
            album_info = {
                'album_id': album.get('id'),
                'album_name': album.get('name'),
                'album_type': album.get('album_type'),
                'artist_id': album['artists'][0]['id'] if album['artists'] else None,
                'available_markets': album.get('available_markets'),
                'release_date': album.get('release_date'),  # Added release date
                'total_tracks': album.get('total_tracks')   # Added total tracks
            }
            transformed_data.append(album_info)

    # Create a DataFrame from our list of dictionaries
    df_albums = pd.DataFrame(transformed_data)

    # Select and rename columns as needed
    df_selected = df_albums[['album_id', 'album_name', 'album_type', 'artist_id', 'release_date', 'total_tracks']]

    return df_selected




def transform_artists(artist_data: list[dict]) -> pd.DataFrame:
    # Normalize the JSON data into a pandas DataFrame
    df_artists = pd.json_normalize(artist_data)

    # Select the relevant columns
    df_selected = df_artists[["id", 'name', 'popularity', 'genres', 'followers.total', 'external_urls.spotify']]

    # Rename the columns for clarity
    df_selected.rename(columns={"id": "artist_id", 'followers.total': 'followers', 'external_urls.spotify': 'spotify_url'}, inplace=True)

    # Convert the list of genres to a string
    df_selected['genres'] = df_selected['genres'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    return df_selected

########################################## LOAD ###############################################




def load(
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
    

