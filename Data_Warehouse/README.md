## Introduction
This is a project introducing the using ways of AWS and how to desgin a data warehouse.
## Purpose
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. This project aims to build an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to
## Database Design
### Staging tables
All of the fields of staging tables are coming from our dataset.
- staging_events
    - artist
    - auth 
    - firstName 
    - gender 
    - itemInSession
    - lastName
    - length
    - level
    - location
    - method
    - page
    - registration
    - sessionId
    - song
    - status
    - ts
    - userAgent
    - userId
- staging_songs
    - num_songs
    - artist_id
    - artist_latitude
    - artist_longitude
    - artist_location
    - artist_name
    - song_id
    - title
    - duration
    - year
### Fact tables
- song_plays 
    - songplay_id primary key
    - start_time distkey -- 1970-01-01:00:00 + timestamp(ms)/1000
    - user_id
    - level
    - song_id sortkey
    - artist_id sortkey
    - session_id
    - location
    - user_agent
### Dimensional tables
To reduces disk I/O and improves query performance, we should use distkey and diststyle to make the nodes work in parallel to speed up query execution. So we should pick the keys which could be used frequently in joinning as sortkey and the column which has many distinct values as distkey.
- users(diststyle all)
    - user_id primary key
    - first_name
    - last_name
    - gender
    - level
- songs
    - song_id primary key sortkey
    - title
    - artist_id
    - year
    - duration
- artists
    - artist_id primary key distkey sortkey
    - name
    - location
    - latitude
    - longitude
- time
    - start_time primary key sortkey
    - hour
    - day
    - week
    - month
    - year
    - weekday
## How to work
This project has the following files:
- Readme.md
    - Introduction of this project
- create_tables.py
    - connect to the cluster and use functions to drop and create tables
- etl.py
    - extract, transform and load the data 
- sql_queries.py
    - defined sql statements
### Step one: create tables
```bash
python create_tables.py
```
### Step two: ETL
``` bash
python etl.py
```
## Analysis
- Proportion of User's gender
    ```sql
    SELECT gender, count(1) FROM users group by gender;

    F:60 M:44
    ```
    - result：Female is more likely to hear the songs
- Time of songplays
    ```sql
    SELECT month, count(1) as times FROM time group by month;

    11, 320
    ```
    - This dataset is not complete