import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from collections import Counter

def get_global_top_40_artists(client_id, client_secret):
    # Authentication
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # Fetch top tracks globally (you can adjust the market if needed)
    top_tracks = sp.playlist_tracks('37i9dQZEVXbMDoHDwVN2tF')  # This is the Global Top 50 playlist ID

    # Extract artist IDs from the tracks
    artist_ids = [track['track']['artists'][0]['id'] for track in top_tracks['items']]

    # Count the frequency of each artist ID
    artist_counts = Counter(artist_ids)

    # Fetch artist details
    top_artists = []
    for artist_id, _ in artist_counts.most_common(40):
        artist = sp.artist(artist_id)
        top_artists.append((artist['name']))#, artist['followers']['total']))

    # Sort artists by follower count, descending
    top_artists.sort(key=lambda x: x[1], reverse=True)

    return top_artists[:40]
