import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts DOUBLE PRECISION,
    userAgent TEXT,
    userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR NOT NULL,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL, 
    duration FLOAT,
    year INT
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR(1),
    level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
    year INT,
    duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIME(6) PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY (0,1) PRIMARY KEY,
    start_time TIME(6) NOT NULL REFERENCES time(start_time),
    user_id INT NOT NULL REFERENCES users(user_id),
    level VARCHAR,
    song_id VARCHAR REFERENCES songs(song_id),
    artist_id VARCHAR REFERENCES artists(artist_id),
    session_id INT NOT NULL,
    location VARCHAR,
    user_agent TEXT
)
""")

# STAGING TABLES
## Data imported from S3 Bucket (JSON files)

staging_events_copy = ("""
COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#copy_table_queries = [staging_events_copy, staging_songs_copy]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
