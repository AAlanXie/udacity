import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSON_PATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


# DROP TABLES
staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"


# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(512),
    auth VARCHAR(64),
    firstName VARCHAR(32),
    gender VARCHAR(2),
    itemInSession INT,
    lastName VARCHAR(32),
    length NUMERIC,
    level VARCHAR(8),
    location VARCHAR(512),
    method VARCHAR(8),
    page VARCHAR(16),
    registration NUMERIC,
    sessionId INT,
    song VARCHAR(512),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(256),
    userId INT
);
""")


# eg. {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR(32),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(512),
    artist_name VARCHAR(512),
    song_id VARCHAR(32),
    title VARCHAR(512),
    duration NUMERIC,
    year INT);
""")

songplay_table_create = ("""
create table if not exists songplays(
    songplay_id int IDENTITY(0,1) primary key,
    start_time timestamp not null distkey,
    user_id int not null,
    level VARCHAR(8),
    song_id varchar(32) not null,
    artist_id varchar(32) not null sortkey,
    session_id int,
    location VARCHAR(512),
    user_agent varchar(256)
);
""")

user_table_create = ("""
create table if not exists users(
    user_id int primary key,
    first_name varchar(32),
    last_name varchar(32),
    gender varchar(2),
    level varchar(8)
)diststyle all;
""")

song_table_create = ("""
create table if not exists songs(
    song_id varchar(32) primary key sortkey,
    title varchar(512),
    artist_id varchar(32) not null,
    year int,
    duration NUMERIC
);
""")

artist_table_create = ("""
create table if not exists artists(
    artist_id varchar(32) primary key distkey sortkey,
    name varchar(512),
    location varchar(512),
    latitude float,
    longitude float
);
""")

time_table_create = ("""
create table if not exists time(
    start_time timestamp primary key sortkey,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
);
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events FROM '{}'
credentials 'aws_iam_role={}'
REGION 'us-west-2'
JSON '{}';
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSON_PATH)

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
credentials 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""").format(SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES

# insert songplays
songplay_table_insert = ("""
insert into songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
    select
    TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
    userId, 
    level, 
    song_id, 
    artist_id, 
    sessionId, 
    location, 
    userAgent
    from staging_events as se
    join staging_songs as ss
    on se.song = ss.title
    and se.length = ss.duration
    and se.artist = ss.artist_name
    and se.page = 'NextSong'
""")

# staging_events
user_table_insert = ("""
insert into users(
    user_id,
    first_name,
    last_name,
    gender,
    level)
    select
    distinct userId,
    firstName, 
    lastName, 
    gender, 
    level
    from staging_events as se
    where se.page = 'NextSong'
""")

# staging_songs join staging_events
song_table_insert = ("""
insert into songs(
    song_id,
    title,
    artist_id,
    year,
    duration)
    select 
    distinct song_id, 
    title, 
    artist_id, 
    year, 
    duration 
    FROM staging_events se
    JOIN staging_songs ss
    ON se.song = ss.title
    AND se.artist = ss.artist_name
""")

# staging_songs
artist_table_insert = ("""
insert into artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
    select 
    distinct artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
    FROM staging_songs;
""")

# time insert
time_table_insert = ("""
insert into time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
    select 
    start_time, 
    EXTRACT(HOUR FROM start_time),
    EXTRACT(DAY FROM start_time),
    EXTRACT(WEEK FROM start_time),
    EXTRACT(MONTH FROM start_time),
    EXTRACT(YEAR FROM start_time),
    EXTRACT(DOW FROM start_time)
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
