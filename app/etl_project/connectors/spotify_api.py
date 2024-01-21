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
            response = requests.get(f'{self.base_url}/artists/{id}', headers=self.headers)
            response.raise_for_status()
            artist = response.json()
        except requests.exceptions.RequestException as e:  # This will catch any Request-related errors
            print(f"Error retrieving artist: {e}")
            return None

        return artist

    def get_tracks(self, id,market):
        market=market.lower()
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


"""" 

load_dotenv()
CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
SERVER_NAME = os.environ.get("SERVER_NAME")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
PORT=os.environ.get("PORT")
manager=SpotifyAPI(CLIENT_ID,CLIENT_SECRET)

playlist_obtained=manager.get_playlist("7jmQBEvJyGHPqKEl5UcEe9")

artist=manager.get_artist("66CXWjxzNUsdJxJ2JdwvnR")

tracks=manager.get_tracks("66CXWjxzNUsdJxJ2JdwvnR",market="US")

albums=manager.get_albums("66CXWjxzNUsdJxJ2JdwvnR")


"""
