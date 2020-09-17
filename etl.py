"""
This module extracts data from ./data/song_data and ./data/log_data json 
files, processes the data, and then inserts it into the tables in the sparkify database
created by the 'create_tables.py' module 
"""

from typing import Callable
from psycopg2.extensions import connection, cursor

import psycopg2

import os
import glob
import pandas as pd


from sql_queries import (
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    song_select,
)


def process_song_file(cur: cursor, filepath: str) -> None:
    """
    Process a given song file and load it to the database

    Parameters
    ----------
    cur: cursor, a cursor object which allows Python to execute PostgreSQL commands in a database
    session
    filepath: str, a string specifying the directory to get file names from

    Returns
    -------
    None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values
    song_data = song_data[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ].values
    artist_data = artist_data[0].tolist()

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur: cursor, filepath: str) -> None:
    """
    Process a log file and load it to the database

    Parameters
    ----------
    cur: cursor, a cursor object which allows Python to execute PostgreSQL commands in a database
    session
    filepath: str, a string specifying the directory to get file names from

    Returns
    -------
    None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    time_data = list(
        zip(
            t.dt.strftime("%Y-%m-%d %I:%M:%S"),
            t.dt.hour,
            t.dt.day,
            t.dt.isocalendar().week,
            t.dt.month,
            t.dt.year,
            t.dt.weekday,
        )
    )
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(time_data, columns=column_labels)

    # pylint: disable=unused-argument
    for index, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[
        ["userId", "firstName", "lastName", "gender", "level"]
    ].drop_duplicates()

    # insert user records
    # pylint: disable=unused-argument
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        start_time = pd.to_datetime(row.ts, unit="ms").strftime("%Y-%m-%d %I:%M:%S")
        songplay_data = (
            start_time,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur: cursor, conn: connection, filepath: str, func: Callable) -> None:
    """
    Process each data file in a given filepath using func

    Parameters
    ----------
    cur: cursor, a cursor object which allows Python to execute PostgreSQL commands in a database
    session
    conn: connection, a connection object which handles the connection to a PostgreSQL database
    instance
    filepath: str, a string specifying the directory to get file names from
    func: callable, a function defined in this module: process_song_file or process_log_file

    Returns
    -------
    None
    """
    # get all files matching extension from directory
    all_files = []
    # pylint: disable=unused-argument
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=postgres password=bracyderek"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
