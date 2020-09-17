"""
This module connects to and creates the sparkifydb if it does not already exists.  It also drops
and creates tables in the sparkifydb as specified by the queries imported from 'sql_queries.py'
"""

from typing import Tuple
from psycopg2.extensions import connection, cursor

import psycopg2

from sql_queries import create_table_queries, drop_table_queries


def create_database() -> Tuple[cursor, connection]:
    """
    Connects to and creates the sparkifydb if it does not already exist.

    Parameters
    ----------
    None

    Returns
    -------
    output: Tuple[cursor, connection]
    """

    # connect to default database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=bracyderek"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=postgres password=bracyderek"
    )
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn) -> None:
    """
    Drops tables in the sparkify database as specified by the queries in 'drop_table_queries'
    list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn) -> None:
    """
    Creates tables in the sparkify database as specified by the queries in 'drop_table_queries'
    list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
