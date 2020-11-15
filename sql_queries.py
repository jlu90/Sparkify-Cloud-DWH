import configparser


# CONFIG INFORMATION
config = configparser.ConfigParser(interpolation=None)
config.read('dwh.cfg')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(255),
    auth VARCHAR(20),
    firstName VARCHAR(100),
    gender CHARACTER,
    itemInSession INT,
    lastName VARCHAR(100),
    length FLOAT,
    level VARCHAR(20),
    location VARCHAR(255),
    method VARCHAR(10),
    page VARCHAR(20),
    registration FLOAT,
    sessionId INT,
    song VARCHAR(255),
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR(20),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(20),
    title VARCHAR(255), 
    duration FLOAT,
    year INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY (0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
    user_id INT NOT NULL REFERENCES users(user_id),
    level VARCHAR(20),
    song_id VARCHAR(20) NOT NULL REFERENCES songs(song_id),
    artist_id VARCHAR(20) NOT NULL REFERENCES artists(artist_id),
    session_id INT,
    location VARCHAR(255),
    user_agent TEXT
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender CHARACTER,
    level VARCHAR(20)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255),
    artist_id VARCHAR(20) NOT NULL REFERENCES artists(artist_id),
    year INT,
    duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR
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
json 'auto' TRUNCATECOLUMNS
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT se.userId, se.firstName, se.lastName, se.gender, se.level
FROM staging_events se
WHERE se.userId IS NOT NULL

""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT s.song_id, s.title, s.artist_id, s.year, s.duration
FROM staging_songs s
WHERE s.song_id IS NOT NULL

""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT s.artist_id, s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude
FROM staging_songs s
WHERE s.artist_id IS NOT NULL

""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT s.start_time, EXTRACT(HOUR FROM s.start_time), EXTRACT(DAY FROM s.start_time), EXTRACT(WEEK FROM s.start_time), EXTRACT(MONTH FROM s.start_time), EXTRACT(YEAR FROM s.start_time), EXTRACT(WEEKDAY FROM s.start_time)
FROM songplays s
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' as start_time, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
FROM staging_events se LEFT JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
WHERE se.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
