from dotenv import load_dotenv
import os
from assets import load_data,extract_artists,transform_artists,get_artist_id,extract_top_tracks,transform_tracks,extract_albums,transform_albums,extract_global_playlist_countries,transform_playlist
import pandas as pd
import psycopg2
from connectors import init_spotify_client


if __name__ == "__main__":
    load_dotenv()
    CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
    CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT=os.environ.get("PORT")
    sp = init_spotify_client(CLIENT_ID, CLIENT_SECRET)

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

    file_path = 'top_40_artists.csv'

    df = pd.read_csv(file_path)

    artists = df['Artist Name']

    #### artist ###
    
    artist_data = extract_artists(sp, artists)

    artist_df = transform_artists(artist_data)


    load_data(artist_df,"artist",
        db_user=DB_USERNAME,
        db_password=DB_PASSWORD,
        db_server_name=SERVER_NAME,
        db_database_name=DATABASE_NAME,
        port=PORT
    )


    artist_ids=get_artist_id(artist_data)

    #### tracks ###

    track_data=extract_top_tracks(sp,artist_data)

    top_track_df = transform_tracks(track_data)

    load_data(top_track_df,"tracks",
        db_user=DB_USERNAME,
        db_password=DB_PASSWORD,
        db_server_name=SERVER_NAME,
        db_database_name=DATABASE_NAME,
        port=PORT

    )

    #### albums ###
    
    # Extract albums
    album_data = extract_albums(sp, artist_ids)

    # Transform album data
    album_df = transform_albums(album_data)

    # Load album data into the database

    load_data(album_df,"albums",
        db_user=DB_USERNAME,
        db_password=DB_PASSWORD,
        db_server_name=SERVER_NAME,
        db_database_name=DATABASE_NAME,
        port=PORT

    )


    #### PLAYLISTS

    markets=["US","ES","PT","BE","GB"]

    playlist_data = extract_global_playlist_countries(sp, markets)

    df_playlist = transform_playlist(playlist_data)

    load_data(df_playlist,"playlists",
        db_user=DB_USERNAME,
        db_password=DB_PASSWORD,
        db_server_name=SERVER_NAME,
        db_database_name=DATABASE_NAME,
        port=PORT

    )



