# %%
import requests

from dotenv import load_dotenv



class SpotifyAPI:
    def __init__(self, client_id: str,client_secret:str):
            # Spotify API URL
        self.base_url = 'https://api.spotify.com/v1'
        if client_id is None:
                raise Exception("API key cannot be set to None.")
        self.client_id = client_id

        self.client_secret = client_secret


        # Get access token
        auth_response = requests.post(
            'https://accounts.spotify.com/api/token',
            data={
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret,
            }
        )

        # Extract the access token from the response
        access_token = auth_response.json()['access_token']

        # Set up the header for subsequent API calls
        self.headers = {
            'Authorization': f'Bearer {access_token}',
        }


    """ 
    def get_playlist(self,playlist_id):
        # This is the Global Top 50 playlist ID
        response = requests.get(f'{self.base_url}/playlists/{playlist_id}/tracks', headers=self.headers)

        # Parse the JSON response
        top_tracks = response.json()

        # [Your existing code to process top_tracks goes here]

        return top_tracks

    def get_artist(self,id):
         
        response = requests.get(f'{self.base_url}/artists/{id}', headers=self.headers)
                                
        artist = response.json()


        return artist 

    """
    def get_playlist(self, playlist_id):
        try:
            response = requests.get(f'{self.base_url}/playlists/{playlist_id}', headers=self.headers)
            response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
            playlistdata = response.json()
        except requests.exceptions.RequestException as e:  # This will catch any Request-related errors
            print(f"Error retrieving playlist: {e}")
            return None

        return playlistdata

    def get_artist(self, id):
        try:
            response = requests.get(f'{self.base_url}/artists/{id}/top-tracks?market={market}', headers=self.headers)
            response.raise_for_status()
            artist = response.json()
        except requests.exceptions.RequestException as e:  # This will catch any Request-related errors
            print(f"Error retrieving artist: {e}")
            return None

        return artist

    def get_tracks_previous(self, id,market):
        market = market.upper()
        try:
            response = requests.get(f'{self.base_url}/artists/{id}/top-tracks?market={market}', headers=self.headers)
            response.raise_for_status()
            tracks = response.json()
        except requests.exceptions.RequestException as e:  # This will catch any Request-related errors
            print(f"Error retrieving tracks: {e}")
            return None

        return tracks
    
    def get_tracks(self, id,market="US"):
        market = market.upper()
        try:
            response = requests.get(f'{self.base_url}/artists/{id}/top-tracks?market={market}', headers=self.headers)
            response.raise_for_status()
            tracks = response.json()
        except requests.exceptions.RequestException as e:  # This will catch any Request-related errors
            print(f"Error retrieving tracks: {e}")
            return None

        return tracks
    
    def get_albums(self, id):
        try:
            response = requests.get(f'{self.base_url}/artists/{id}/albums', headers=self.headers)
            response.raise_for_status()
            albums_data = response.json()
        except requests.exceptions.RequestException as e:  # This will catch any Request-related errors
            print(f"Error retrieving albums: {e}")
            return None

        return albums_data   



    def spotify_api_request(self):
        """ Function to handle Spotify API requests. """
        response = requests.get(self.base_url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            # Handle errors (e.g., rate limits, invalid requests)
            return None



#################################### Extract ########################################################

# Example extract function for artists

import os
import pandas as pd
def extract_top_tracks_single(spotif_client: SpotifyAPI, artist_id: list ,market:str = "US") -> list[dict]:
    results = spotif_client.get_tracks(artist_id,market) ### which country is this from?
    return results


def extract_top_tracks(spotify_client: SpotifyAPI, artist_ids: list[str], markets: list[str]) -> list[tuple[dict, str]]:
    top_tracks_data = []
    for artist_id in artist_ids:
        tracks = extract_top_tracks_single(spotify_client, artist_id)
        top_tracks_data.append(tracks["tracks"])
    return top_tracks_data







def transform_tracks(track_data: list[dict]) -> pd.DataFrame:
    df_tracks = pd.json_normalize(track_data)
    print(df_tracks)
    """
    df_selected = df_tracks[['id', 'name', 'artists', 'album.id', 'duration_ms', 'popularity', 'explicit', 'preview_url', 'track_number']]
    
    # Use .loc[] for assignments
    df_selected.loc[:, 'artist_id'] = df_selected['artists'].apply(lambda x: x[0]['id'] if x else None)
    df_selected.loc[:, 'album_id'] = df_selected['album.id']
    df_selected.rename(columns={"id": "track_id", 'album.id': 'album_id'}, inplace=True)

    return df_selected[['track_id', 'name', 'duration_ms', 'popularity', 'explicit', 'preview_url', 'track_number', 'artist_id', 'album_id']]

    """
    return df_tracks











load_dotenv()
CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
SERVER_NAME = os.environ.get("SERVER_NAME")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
PORT=os.environ.get("PORT")


""" 
#sp = init_spotify_client(CLIENT_ID, CLIENT_SECRET)
spotif_client=SpotifyAPI(CLIENT_ID,CLIENT_SECRET)



# Load the CSV file
file_path="etl_project/data/top_artist_from_global50.csv"

df = pd.read_csv(file_path)

artists_id = df['artist_id']

#### artist ###
market = [
        "GB"
    ]


artist_data=extract_top_tracks(spotif_client, artists_id,market)

#print(artist_data)


# Assuming artist_data is your JSON data
json_info = artist_data
print(json_info)
df_artists = transform_tracks(json_info)


"""
spotif_client=SpotifyAPI(CLIENT_ID,CLIENT_SECRET)

import pandas as pd



# we have to test the extract algums again
def extract_albums(spotify_client: SpotifyAPI, artist_ids: list) -> list[dict]:
    album_data = []
    for artist_id in artist_ids:
        url = spotify_client.get_albums(artist_id)  


# Load the CSV file
file_path="etl_project/data/top_artist_from_global50.csv"

df = pd.read_csv(file_path)

artists_id = df['artist_id']
playlist_dict = {
        "SE": "7jmQBEvJyGHPqKEl5UcEe9",

    }





# Fetching the album data

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


album_data = extract_albums(spotif_client,["66CXWjxzNUsdJxJ2JdwvnR"])

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
    df_selected = df_albums[['album_id', 'album_name',  'artist_id', 'release_date', 'total_tracks']]

    return df_selected


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

# Fetching the album data
album_data = extract_albums(spotif_client, ["66CXWjxzNUsdJxJ2JdwvnR"])

if album_data:
    df_albums = transform_albums(album_data)
    print(df_albums)
else:
    print("Failed to fetch album data")



