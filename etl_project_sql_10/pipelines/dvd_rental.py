from jinja2 import Environment, FileSystemLoader
from etl_project_sql_10.connectors.postgresql import PostgreSqlClient
from dotenv import load_dotenv
import os
from etl_project_sql_10.assets.extract_load_transform import (
    extract_load,
    transform,
    SqlTransform,
)
from graphlib import TopologicalSorter
from etl_project_sql_10.assets.pipeline_logging import PipelineLogging
from etl_project_sql_10.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
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

def run_pipeline(pipeline_config: dict, postgresql_logging_client: PostgreSqlClient):
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

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    # set schedule
    schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
        run_pipeline,
        pipeline_config=pipeline_config,
        postgresql_logging_client=postgresql_logging_client,
    )

    while True:
        schedule.run_pending()
        time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
