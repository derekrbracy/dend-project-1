# DROP TABLES
# The following SQL queries drop all the tables from sparkifydb.

songplay_table_drop = "DROP TABLE IF EXISTS Songplays"
user_table_drop = "DROP TABLE IF EXISTS Users"
song_table_drop = "DROP TABLE IF EXISTS Songs"
artist_table_drop = "DROP TABLE IF EXISTS Artists"
time_table_drop = "DROP TABLE IF EXISTS Time"

# CREATE TABLES
# The following SQL queries create tables to sparkifydb.

songplay_table_create = """
CREATE TABLE IF NOT EXISTS Songplays(
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR);
"""

user_table_create = """
CREATE TABLE Users(
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR NOT NULL);
"""

song_table_create = """
CREATE TABLE Songs(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration NUMERIC);
"""

artist_table_create = """
CREATE TABLE Artists(
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC);
"""

time_table_create = """
CREATE TABLE Time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT);
"""

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location, 
    user_agent)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
"""

user_table_insert = """
INSERT INTO users(
    user_id,
    first_name,
    last_name,
    gender,
    level)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT(user_id)
    DO UPDATE
        SET level=excluded.level;
"""

song_table_insert = """
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT(song_id)
    DO NOTHING;
"""

artist_table_insert = """
INSERT INTO artists(
    artist_id,
    name,
    location,
    latitude,
    longitude)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT(artist_id)
    DO NOTHING;
"""

time_table_insert = """
INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
VALUES(%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(start_time)
    DO NOTHING;
"""

# FIND SONGS

song_select = """
SELECT
    s.song_id,
    s.artist_id
FROM
    songs s
JOIN artists a ON s.artist_id = a.artist_id
WHERE
    s.title = %s
    AND a.name = %s
    AND s.duration = %s
"""

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]

drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
