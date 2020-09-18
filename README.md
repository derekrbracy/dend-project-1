_[Udacity's Data Engineer Nano Degree](https://eu.udacity.com/course/data-engineer-nanodegree--nd027)_ | derekrbracy@gmail.com | 2020-09-16

# PROJECT-1: Data Modelling with Postgres

## Overview

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaing app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

This project contains a collection of scripts which process data and create and store that data in a Postgres database with tables designed to optimize queries on song play analyis.

## Data

 There are two different JSON file types to process.

**./data/song_data**: static data about artists and songs

```json
{
  "num_songs": 1,
  "artist_id": "ARMJAGH1187FB546F3",
  "artist_latitude": 35.14968,
  "artist_longitude": -90.04892,
  "artist_location": "Memphis, TN",
  "artist_name": "The Box Tops",
  "song_id": "SOCIWDW12A8C13D406",
  "title": "Soul Deep",
  "duration": 148.03546,
  "year": 1969
}
```

**./data/log_data**: event data of service usage e.g. who listened what song, when, where, and with which client

```json
{
  "artist":"Hoobastank"
  ,"auth":"Logged In"
  ,"firstName":"Cierra"
  ,"gender":"F",
  "itemInSession":0,
  "lastName":"Finley",
  "length":241.3971,
  "level":"free",
  "location":"Richmond, VA",
  "method":"PUT",
  "page":"NextSong",
  "registration":1541013292796.0,
  "sessionId":132,
  "song":"Say The Same",
  "status":200,"ts":1541808927796,
  "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.77.4 (KHTML, like Gecko) Version\/7.0.5 Safari\/537.77.4\"",
  "userId":"96"
}
```

## Database

https://docs.microsoft.com/en-us/power-bi/guidance/star-schema

The Sparkify analytics database (sparkifydb) has a star schema design. Star schema is a mature modeling approach widely adopted by relational data warehouses. It requires modelers to classify their model tables as either dimension or fact.


**Dimension tables** describe business entities—the things you model. Entities can include products, people, places, and concepts including time itself. The most consistent table you'll find in a star schema is a date dimension table. A dimension table contains a key column (or columns) that acts as a unique identifier, and descriptive columns.

**Fact Tables** store observations or events, store observations or events, and can be sales orders, stock balances, exchange rates, temperatures, etc. A fact table contains dimension key columns that relate to dimension tables, and numeric measure columns. The dimension key columns determine the dimensionality of a fact table, while the dimension key values determine the granularity of a fact table. For example, consider a fact table designed to store sale targets that has two dimension key columns Date and ProductKey. It's easy to understand that the table has two dimensions. The granularity, however, can't be determined without considering the dimension key values. In this example, consider that the values stored in the Date column are the first day of each month. In this case, the granularity is at month-product level.

### Fact Table

* **songplays**: song play data together with user, artist, and song info

### Dimension Tables

* **users**: user info
* **songs**: song info 
* **artists**: artist info
* **time**: detailed time info about song plays

## Schema
https://ibb.co/2M6h4Dg

![Sparkifydb schema as Entity Relationship Diagram](/udacity-project-1-diagram.png)

_*sparkifydb entity relationship diagram*_

---

## How to

### Prerequisites

* **Python** (https://www.python.org/) for package depedencies see requirements.txt

* **Jupyter Notebook** (https://jupyter.org/)

* **Postgresql** (https://www.postgresql.org/)

_*There are many great existing tutorials on how to install Python, Jupyter notebook, and Postgresql, as such details are omitted here.*_

### Create tables in the database

Open a command prompt and type

```
python create_tables.py
```
This script connects to and creates the Sparkifydb and creates and drops tables

### Process the data and load to the database

```
python etl.py
```
This script extracts data from ./data/song_data and ./data/log_data json files, processes the data, and then inserts it into the tables in the database


### Interacting with the database




<I> Which is the most used user agent (web browser) to play songs?</I>
#### Example Query
``` SQL
SELECT user_agent, count(user_agent) FROM songplays GROUP BY user_agent;
```

