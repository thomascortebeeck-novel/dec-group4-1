from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader, Template
from graphlib import TopologicalSorter

from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader, Template
import psycopg2
import logging
import psycopg2
import logging

def extract(
    sql_template: Template,
    source_engine: Engine,
    target_engine: Engine,
) -> list[dict]:
    extract_type = sql_template.make_module().config.get("extract_type")
    if extract_type == "full":
        sql = sql_template.render()
        return [dict(row) for row in source_engine.execute(sql).all()]
    elif extract_type == "incremental":
        # if target table exists :
        source_table_name = sql_template.make_module().config.get("source_table_name")
        if inspect(target_engine).has_table(source_table_name):
            incremental_column = sql_template.make_module().config.get(
                "incremental_column"
            )
            sql_result = [
                dict(row)
                for row in target_engine.execute(
                    f"select max({incremental_column}) as incremental_value from {source_table_name}"
                ).all()
            ]
            incremental_value = sql_result[0].get("incremental_value")
            sql = sql_template.render(
                is_incremental=True, incremental_value=incremental_value
            )
        else:
            sql = sql_template.render(is_incremental=False)
        print(sql)
        return [dict(row) for row in source_engine.execute(sql).all()]
    else:
        raise Exception(
            f"Extract type {extract_type} is not supported. Please use either 'full' or 'incremental' extract type."
        )


def get_schema_metadata(engine: Engine) -> Table:
    metadata = MetaData(bind=engine)
    metadata.reflect()  # get target table schemas into metadata object
    return metadata


def _create_table(table_name: str, metadata: MetaData, engine: Engine):
    existing_table = metadata.tables[table_name]
    new_metadata = MetaData()
    columns = [
        Column(column.name, column.type, primary_key=column.primary_key)
        for column in existing_table.columns
    ]
    new_table = Table(table_name, new_metadata, *columns)
    new_metadata.create_all(bind=engine)
    return new_metadata


def load(
    data: list[dict],
    table_name: str,
    engine: Engine,
    source_metadata: MetaData,
    chunksize: int = 1000,
):
    target_metadata = _create_table(
        table_name=table_name, metadata=source_metadata, engine=engine
    )
    table = target_metadata.tables[table_name]
    max_length = len(data)
    key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]
    for i in range(0, max_length, chunksize):
        if i + chunksize >= max_length:
            lower_bound = i
            upper_bound = max_length
        else:
            lower_bound = i
            upper_bound = i + chunksize
        insert_statement = postgresql.insert(table).values(
            data[lower_bound:upper_bound]
        )
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        engine.execute(upsert_statement)


class SqlTransform:
    def __init__(self, engine: Engine, environment: Environment, table_name: str):
        self.engine = engine
        self.environment = environment
        self.table_name = table_name
        self.template = self.environment.get_template(f"{table_name}.sql")

    def create_table_as(self) -> None:
        """
        Drops the table if it exists and creates a new copy of the table using the provided select statement.
        """
        exec_sql = f"""
            drop table if exists {self.table_name};
            create table {self.table_name} as (
                {self.template.render()}
            )
        """
        self.engine.execute(exec_sql)



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



if __name__ == "__main__":
    load_dotenv()
    print("hello")

    SOURCE_DATABASE_NAME = os.environ.get("DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SOURCE_PORT = os.environ.get("PORT")


    source_connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=SOURCE_DB_USERNAME,
        password=SOURCE_DB_PASSWORD,
        host=SOURCE_SERVER_NAME,
        port=SOURCE_PORT,
        database=SOURCE_DATABASE_NAME,
    )
    source_engine = create_engine(source_connection_url)

    TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    TARGET_PORT = os.environ.get("TARGET_PORT")


    create_database(TARGET_SERVER_NAME, TARGET_DB_USERNAME, TARGET_DB_PASSWORD, TARGET_DATABASE_NAME, TARGET_PORT)


    target_connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=TARGET_DB_USERNAME,
        password=TARGET_DB_PASSWORD,
        host=TARGET_SERVER_NAME,
        port=TARGET_PORT,
        database=TARGET_DATABASE_NAME,
    )
    target_engine = create_engine(target_connection_url)

    extract_environment = Environment(loader=FileSystemLoader("sql/extract"))

    for sql_path in extract_environment.list_templates():
        sql_template = extract_environment.get_template(sql_path)
        table_name = sql_template.make_module().config.get("source_table_name")
        data = extract(
            sql_template=sql_template,
            source_engine=source_engine,
            target_engine=target_engine,
        )
        source_metadata = get_schema_metadata(engine=source_engine)
        load(
            data=data,
            table_name=table_name,
            engine=target_engine,
            source_metadata=source_metadata,
        )

    transform_environment = Environment(loader=FileSystemLoader("sql/transform_artist"))

    serving_sales_cumulative = SqlTransform(
        engine=target_engine,
        environment=transform_environment,
        table_name="staging_artists",
    )
    serving_sales_month_end = SqlTransform(
        engine=target_engine,
        environment=transform_environment,
        table_name="staging_playlists",
    )



    dag = TopologicalSorter()
    dag.add(serving_sales_cumulative)
    dag.add(serving_sales_month_end)




    for node in tuple(dag.static_order()):
        node.create_table_as()




