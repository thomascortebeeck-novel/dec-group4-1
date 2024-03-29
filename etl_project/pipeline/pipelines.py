#from dotenv import load_dotenv
#import os
#from etl_project.assets.spotify_assets import load_data,extract_artists,transform_artists,get_artist_id,extract_top_tracks,transform_tracks,extract_albums,transform_albums,extract_global_playlist_countries,transform_playlist
import pandas as pd
import psycopg2
#from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, ForeignKey, Boolean
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, ForeignKey, Boolean
from sqlalchemy import DateTime
import datetime
from jinja2 import Environment, FileSystemLoader, Template
from sqlalchemy.engine import URL, Engine
from pathlib import Path
import yaml
from dotenv import load_dotenv
import os
from etl_project.assets.transformations import SqlTransform
from etl_project.assets.spotify_assets import (
    extract_artists,
    transform_artists,
    load,
    extract_top_tracks,
    transform_tracks,
    extract_global_playlist_countries,
    transform_playlist,
    extract_albums,
    transform_albums, 
)
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.spotify_api import SpotifyAPI
from sqlalchemy import Table, MetaData, Column, Integer, String, Float

import psycopg2
import logging
import schedule
import time
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv
import os
from etl_project.assets.extract_load_transform import (
    extract_load,
    transform,
    SqlTransform,
)
from graphlib import TopologicalSorter
from pathlib import Path
import schedule
import time
import yaml
import psycopg2
import logging



def create_database(server_name, db_username, db_password, database_name, port):
    try:
        # Connect to the PostgreSQL server (default database)
        conn = psycopg2.connect(
            host=server_name,
            user=db_username,
            password=db_password,
            dbname="postgres",  # Connect to the default database
            port=port
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if the target database exists
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{database_name}'")
        exists = cursor.fetchone()
        if not exists:
            # Create the database if it doesn't exist
            cursor.execute(f"CREATE DATABASE {database_name}")
            logging.info(f"Database {database_name} created successfully.")
        else:
            logging.info(f"Database {database_name} already exists.")

    except psycopg2.Error as e:
        logging.error(f"Error while connecting to PostgreSQL: {e}")
    finally:
        # Close the connection and cursor
        if cursor:
            cursor.close()
        if conn:
            conn.close()






def pipeline(config: dict, pipeline_logging: PipelineLogging):

    pipeline_logging.logger.info("Starting pipeline run")
    # set up environment variables
    pipeline_logging.logger.info("Getting pipeline environment variables")
    CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
    CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
    SOURCE_DATABASE_NAME = os.environ.get("SOURCE_DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("SOURCE_SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("SOURCE_DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD")
    SOURCE_PORT = os.environ.get("SOURCE_PORT")

    pipeline_logging.logger.info("Initializing Spotify Client")
    spotif_client=SpotifyAPI(CLIENT_ID,CLIENT_SECRET)


    file_path=config.get("artist_id_folder_path")

    df = pd.read_csv(file_path)

    artists_id = df['artist_id']

    #### artist ###
    pipeline_logging.logger.info("Extracting artist data from Spotify API")
    artist_data=extract_artists(spotif_client, artists_id)

    pipeline_logging.logger.info("Transforming artist data")
    artist_df = transform_artists(artist_data)

    create_database(SOURCE_SERVER_NAME, SOURCE_DB_USERNAME, SOURCE_DB_PASSWORD, SOURCE_DATABASE_NAME, SOURCE_PORT)

    postgresql_client = PostgreSqlClient(
        server_name=SOURCE_SERVER_NAME,
        database_name=SOURCE_DATABASE_NAME,
        username=SOURCE_DB_USERNAME,
        password=SOURCE_DB_PASSWORD,
        port=SOURCE_PORT,
    )

    metadata = MetaData()


    artist_table = Table(
        "artists",
        metadata,
        Column('id', Integer, primary_key=True),  # Auto-incrementing primary key
        Column('artist_id', String),
        Column('name', String),
        Column('popularity', Integer),
        Column('genres', String),
        Column('followers', Integer),
        Column('spotify_url', String),
        Column('load_date', DateTime, default=datetime.datetime.utcnow),
        # schema='raw'
    )


    pipeline_logging.logger.info("Loading transformed artist data to Spoty database")
    load(
        df=artist_df,
        postgresql_client=postgresql_client,
        table=artist_table,
        metadata=metadata,
        load_method="insert",
    )


    #### tracks ###
    pipeline_logging.logger.info("Extracting tracks data from Spotify API")

    market=["US"]

    track_data=extract_top_tracks(spotif_client,artists_id,market)

    pipeline_logging.logger.info("Transforming tracks data")

    top_track_df = transform_tracks(track_data)


    tracks_table = Table(
        'tracks', metadata,
        Column('id', Integer, primary_key=True),  # Auto-incrementing primary key
        Column('track_id', String),
        Column('name', String),
        Column('artist_id', String),
        Column('album_id', String),
        Column('duration_ms', Integer),
        Column('popularity', Integer),
        Column('explicit', Boolean),
        Column('preview_url', String),
        Column('track_number', Integer),
        Column('market', String),
        Column('load_date', DateTime, default=datetime.datetime.utcnow),
            #schema='raw'
    )

    pipeline_logging.logger.info("Loading transformed tracks data to Spoty database")

    load(
        df=top_track_df,
        postgresql_client=postgresql_client,
        table=tracks_table,
        metadata=metadata,
        load_method="insert",
    )



    #### albums ###

    # Extract albums
    pipeline_logging.logger.info("Extracting albums data from Spotify API")

    album_data = extract_albums(spotif_client, artists_id)

    # Transform album data
    pipeline_logging.logger.info("Transforming albums data")

    album_df = transform_albums(album_data)
    #print(album_df)



    albums_table = Table(
        'albums', metadata,
        Column('id', Integer, primary_key=True),  # Auto-incrementing primary key
        Column('album_id', String),
        Column('album_name', String),
        Column('album_type', String),
        Column('artist_id', String),
        Column('release_date', String),
        Column('total_tracks', Integer),
        Column('load_date', DateTime, default=datetime.datetime.utcnow),
            #schema='raw'
        #schema?
    )

    # Load album data into the database
    pipeline_logging.logger.info("Loading transformed albums data to Spoty database")

    load(
        df=album_df,
        postgresql_client=postgresql_client,
        table=albums_table,
        metadata=metadata,
        load_method="insert",
    )





    """

    playlist_dict = {
        "SE": "7jmQBEvJyGHPqKEl5UcEe9",
        "GB": "37i9dQZEVXbLnolsZ8PSNw",
        "BE": "37i9dQZEVXbJNSeeHswcKB",
        "USA":"37i9dQZEVXbLRQDuF5jeBp",
        "PT":"37i9dQZEVXbKyJS56d1pgi"
    }

    """

    pipeline_logging.logger.info("Extracting playlist data from Spotify API")

    playlist_dict = {
        "SE": "7jmQBEvJyGHPqKEl5UcEe9"
    }
    playlist_data = extract_global_playlist_countries(spotif_client,playlist_dict)
    # print(playlist_data)  
    pipeline_logging.logger.info("Transforming playlist data")

    df_playlist = transform_playlist(playlist_data)


    playlist_global = Table(
        'playlist', metadata,
        Column('id', Integer, primary_key=True),  # Auto-incrementing primary key
        Column('artist_id', String),
        Column('playlist_country', String)
    )

    
    pipeline_logging.logger.info("Loading transformed playlists data to Spoty database")
    load(
        df=df_playlist,
        postgresql_client=postgresql_client,
        table=playlist_global,
        metadata=metadata,
        load_method="insert",
    )



    pipeline_logging.logger.info("Pipeline run successful")



def run_pipeline_sql(pipeline_config: dict, postgresql_logging_client: PostgreSqlClient):
    metadata_logging = MetaDataLogging(
        pipeline_name=pipeline_config.get("name"),
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config"),
    )
    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_config.get("name"),
        log_folder_path=pipeline_config.get("config").get("log_folder_path"),
    )

    SOURCE_DATABASE_NAME = os.environ.get("SOURCE_DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("SOURCE_SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("SOURCE_DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD")
    SOURCE_PORT = os.environ.get("SOURCE_PORT")
    TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    TARGET_PORT = os.environ.get("TARGET_PORT")
    try:
        metadata_logging.log()  # start run
        pipeline_logging.logger.info("Creating source client")

        create_database(SOURCE_SERVER_NAME, SOURCE_DB_USERNAME, SOURCE_DB_PASSWORD, SOURCE_DATABASE_NAME, SOURCE_PORT)

        source_postgresql_client = PostgreSqlClient(
            server_name=SOURCE_SERVER_NAME,
            database_name=SOURCE_DATABASE_NAME,
            username=SOURCE_DB_USERNAME,
            password=SOURCE_DB_PASSWORD,
            port=SOURCE_PORT,
        )
        pipeline_logging.logger.info("Creating target client")

        create_database(TARGET_SERVER_NAME, TARGET_DB_USERNAME, TARGET_DB_PASSWORD, TARGET_DATABASE_NAME, TARGET_PORT)

        target_postgresql_client = PostgreSqlClient(
            server_name=TARGET_SERVER_NAME,
            database_name=TARGET_DATABASE_NAME,
            username=TARGET_DB_USERNAME,
            password=TARGET_DB_PASSWORD,
            port=TARGET_PORT,
        )

        extract_template_environment = Environment(
            loader=FileSystemLoader(
                pipeline_config.get("config").get("extract_template_path")
            )
        )
        pipeline_logging.logger.info("Perform extract and load")
        extract_load(
            template_environment=extract_template_environment,
            source_postgresql_client=source_postgresql_client,
            target_postgresql_client=target_postgresql_client,
        )

        transform_template_environment = Environment(
            loader=FileSystemLoader(
                pipeline_config.get("config").get("transform_template_path")
            )
        )

        # create nodes
        staging_films = SqlTransform(
            table_name="staging_playlists",
            postgresql_client=target_postgresql_client,
            environment=transform_template_environment,
        )
        serving_sales_customer = SqlTransform(
            table_name="staging_artists",
            postgresql_client=target_postgresql_client,
            environment=transform_template_environment,
        )

        # create DAG
        dag = TopologicalSorter()
        dag.add(staging_films)
        dag.add(serving_sales_customer)
        # run transform
        pipeline_logging.logger.info("Perform transform")
        transform(dag=dag)
        pipeline_logging.logger.info("Pipeline complete")
        metadata_logging.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline failed with exception {e}")
        metadata_logging.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )
        pipeline_logging.logger.handlers.clear()








def run_pipeline(
    pipeline_name: str,
    postgresql_logging_client: PostgreSqlClient,
    pipeline_config: dict,
):
    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_config.get("name"),
        log_folder_path=pipeline_config.get("config").get("log_folder_path"),
    )
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config"),
    )
    try:
        metadata_logger.log()  # log start
        pipeline(
            config=pipeline_config.get("config"), pipeline_logging=pipeline_logging
        )
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )  # log end
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )  # log error
        pipeline_logging.logger.handlers.clear()





if __name__ == "__main__":
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    create_database(LOGGING_SERVER_NAME, LOGGING_USERNAME, LOGGING_PASSWORD, LOGGING_DATABASE_NAME, LOGGING_PORT)

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )
    
    # Load and validate YAML configuration for the pipelines
    yaml_file_path = __file__.replace(".py", ".yaml")
    if not Path(yaml_file_path).exists():
        raise Exception(f"Missing {yaml_file_path} file! Please create the yaml file.")
    with open(yaml_file_path) as yaml_file:
        config = yaml.safe_load(yaml_file)

    # For the first pipeline
    pipelines_config = config.get("pipelines")
    if not pipelines_config:
        raise Exception("Configuration for 'pipelines' is required.")

    # For the SQL pipeline
    pipeline_sql_config = config.get("pipeline_sql")
    if not pipeline_sql_config:
        raise Exception("Configuration for 'pipeline_sql' is required.")

    # Schedule for the normal pipeline
    schedule.every(pipelines_config.get("schedule").get("run_minutes")).minutes.do(
        run_pipeline,
        pipeline_name=pipelines_config.get("name"),
        postgresql_logging_client=postgresql_logging_client,
        pipeline_config=pipelines_config,
    )

    # Schedule for the SQL pipeline
    schedule.every(pipeline_sql_config.get("schedule").get("run_seconds")).seconds.do(
        run_pipeline_sql,
        pipeline_config=pipeline_sql_config,
        postgresql_logging_client=postgresql_logging_client,
    )

    # Run scheduled tasks
    while True:
        schedule.run_pending()
        time.sleep(min(pipelines_config.get("schedule").get("poll_seconds"),
                       pipeline_sql_config.get("schedule").get("poll_seconds")))
