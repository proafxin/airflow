"""
Read a table from a MySQL database and export as a dataframe
"""

import os

import pandas as pd
import pandera as pa

# from mysql.connector import Error, connect
from pandera.typing import Series
from sqlalchemy import create_engine

from etl.calculate_mean import calculate_mean_by_col

# from sqlalchemy.engine import URL


def get_connection():
    """Read credentials from environment variable and return the mysql connection

    :return: MySQL Connection
    :rtype: connect
    """

    username = os.environ["MYSQL_USERNAME"]
    password = os.environ["MYSQL_PASSWORD"]
    port = os.environ["MYSQL_PORT"]
    port = int(port)
    print(port, type(port))
    dbname = os.environ["AIRFLOW_DBNAME"]
    host = os.environ["MYSQL_HOST"]

    # connection_string = """DRIVER=\{ODBC Driver 17 for SQL Server};"""
    # connection_string = (
    #     f"{connection_string}SERVER={host};DATABASE={dbname};UID={username};PWD={password}"
    # )
    # connection_url = URL.create(f"mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}")
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}?charset=utf8"
    )

    connection = engine.connect()
    return connection


def read_table_from_db(table_name: str, connection):
    """Read table_name from database using connection

    :param table_name: Name of the table.
    :type table_name: str
    :param connection: MySQL connection
    :type connection: _type_
    :return: Dataframe of table data.
    :rtype: pd.DataFrame
    """

    data = pd.read_sql(f"SELECT * FROM {table_name}", connection)

    return data


class MovieSchema(pa.SchemaModel):
    """Movie schema for validation"""

    name: Series[str] = pa.Field(nullable=False)
    description: Series[str] = pa.Field(nullable=False)
    movie_id: Series[int] = pa.Field(le=5, ge=1, nullable=False)


class RatingSchema(pa.SchemaModel):
    """Rating schema for validation"""

    id: Series[int] = pa.Field(nullable=False, ge=1)
    rating: Series[int] = pa.Field(nullable=False, le=5, ge=1)
    user_id: Series[int] = pa.Field(nullable=False, ge=1)
    movie_id: Series[int] = pa.Field(nullable=False, ge=1)


def get_joined_data(connection):
    """Join movies and ratings based on movie_id

    :return: Joined dataframe.
    :rtype: pd.DataFrame
    """

    movies = read_table_from_db(table_name="movies", connection=connection)
    MovieSchema.validate(movies)

    ratings = read_table_from_db(table_name="ratings", connection=connection)
    RatingSchema.validate(ratings)

    joined_data = pd.merge(
        left=movies,
        right=ratings.drop("id", axis=1),
        left_on="movie_id",
        right_on="movie_id",
        how="inner",
    )

    return joined_data


def write_to_table(connection, table_name: str, data: pd.DataFrame):
    """Write dataframe to table using SQLAlchemy connection

    :param connection: SQLAlchemy connection
    :type connection: connect
    :param table_name: Table to write data
    :type table_name: str
    :param data: Dataframe to write
    :type data: pd.DataFrame
    """

    data.to_sql(con=connection, name=table_name, if_exists="replace")


def run_pipeline(connection):
    """Get average by movie and average by user

    :return: Dataframe of mean by movie, dataframe of mean by user
    :rtype: Tuple[pd.DataFrame, pd.DataFrame]
    """

    data = get_joined_data(connection=connection)
    print(data)
    movie_mean = calculate_mean_by_col(data=data, groupby_cols=["movie_id"], groupby_on="rating")
    print(movie_mean)
    user_mean = calculate_mean_by_col(data=data, groupby_cols=["user_id"], groupby_on="rating")
    print(user_mean)
    write_to_table(connection=connection, table_name="movie_avg", data=movie_mean)
    write_to_table(connection=connection, table_name="user_avg", data=user_mean)

    # return movie_mean, user_mean
