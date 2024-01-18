import unittest
from unittest.mock import patch, Mock
from etl_project.connectors.spotify_api import SpotifyAPI  # Replace with the actual name of your module
from dotenv import load_dotenv
import os
import requests
class TestSpotifyAPI(unittest.TestCase):

    def setUp(self):
        load_dotenv()
        CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
        CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
        self.client_id = CLIENT_ID  # Replace with your client ID
        self.client_secret = CLIENT_SECRET  # Replace with your client secret
        self.spotify_api = SpotifyAPI(self.client_id, self.client_secret)

    @patch('requests.get')
    def test_get_artist(self, mock_get):
        # Mocking the API response
        mock_response = Mock()
        expected_output = {
            "external_urls": {"spotify": "string"},
            "followers": {"href": "string", "total": 0},
            "genres": ["Prog rock", "Grunge"],
            "href": "string",
            "id": "string",
            "images": [
                {
                    "url": "https://i.scdn.co/image/ab67616d00001e02ff9ca10b55ce82ae553c8228",
                    "height": 300,
                    "width": 300
                }
            ],
            "name": "string",
            "popularity": 0,
            "type": "artist",
            "uri": "string"
        }

        mock_response.json.return_value = expected_output
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call the method
        artist_id = "0TnOYISbd1XYRBk9myaseg"  # Example artist ID
        result = self.spotify_api.get_artist(artist_id)

        # Assertions to check if the output is as expected
        self.assertEqual(result, expected_output)

    @patch('requests.get')
    def test_get_artist_with_exception(self, mock_get):
        # Mocking an exception being raised during the API call
        mock_get.side_effect = requests.exceptions.RequestException("Invalid artist ID")

        # Call the method with an invalid artist ID
        invalid_artist_id = "invalid_id"
        result = self.spotify_api.get_artist(invalid_artist_id)

        # Assertions to check if the output is None due to the exception
        self.assertIsNone(result, "Expected None when an exception occurs")


if __name__ == '__main__':
    unittest.main()
