# contains drop table, create table, insert table and select statement queries
import configparser

# CONFIG
# dwh.cfg file contains data for Redshift Cluster, S3 and IAM_ROLE
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE STAGING TABLES (1. staging_events and 2. staging_songs) in S3

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
        artist            VARCHAR       NULL,
        auth              VARCHAR       NULL, 
        firstName         VARCHAR       NULL,
        gender            VARCHAR       NULL,
        itemInSession     INTEGER       NULL,
        lastName          VARCHAR       NULL,
        length            FLOAT         NULL,
        level             VARCHAR       NULL,
        location          VARCHAR       NULL,
        method            VARCHAR       NULL,
        page              VARCHAR       NULL,
        registration      FLOAT         NULL,
        sessionId         INTEGER       NOT NULL,
        song              VARCHAR       NULL,
        status            INTEGER       NULL,
        ts                BIGINT        NOT NULL,
        userAgent         VARCHAR       NULL,
        userId            INTEGER       NULL
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
       song_id            VARCHAR         NOT NULL,
       artist_id          VARCHAR         NOT NULL,
       artist_latitude    FLOAT           NULL,
       artist_longitude   FLOAT           NULL,
       artist_location    VARCHAR         NULL,
       artist_name        VARCHAR         NULL,
       duration           FLOAT           NULL,
       num_songs          INTEGER         NULL,
       title              VARCHAR         NULL,
       year               INTEGER         NULL
)
""")

#### Create Fact Table -- songplays
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
        songplay_id     INTEGER IDENTITY(0,1)     NOT NULL SORTKEY,
        start_time      TIMESTAMP                 NOT NULL,
        user_id         INTEGER                   NOT NULL,
        level           VARCHAR                   NOT NULL,
        song_id         VARCHAR                   NOT NULL,
        artist_id       VARCHAR                   NOT NULL DISTKEY,
        session_id      INTEGER                   NOT NULL,
        location        VARCHAR                   NULL,
        user_agent      VARCHAR                   NULL
)
""")


###### Create Dimension table -- users
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
       user_id      INTEGER            NOT NULL SORTKEY,
       first_name   VARCHAR            NULL,
       last_name    VARCHAR            NULL,
       gender       VARCHAR            NULL,
       level        VARCHAR            NULL
)
diststyle ALL;
""")

###### Create Dimension table -- songs
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
       song_id      VARCHAR          NOT NULL SORTKEY,
       title        VARCHAR          NOT NULL,
       artist_id    VARCHAR          NOT NULL,
       year         INTEGER          NOT NULL,
       duration     FLOAT            NOT NULL
)
""")

###### Create Dimension table -- artists
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
       artist_id     VARCHAR        NOT NULL SORTKEY,
       name          VARCHAR        NULL,
       location      VARCHAR        NULL,
       latitude      FLOAT          NULL,
       longitude     FLOAT          NULL
)
diststyle ALL;
""")

###### Create Dimension table -- time
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
       start_time TIMESTAMP    NOT NULL SORTKEY,
       hour       INTEGER      NULL,
       day        INTEGER      NULL,
       week       INTEGER      NULL,
       month      INTEGER      NULL,
       year       INTEGER      NULL,
       weekday    INTEGER      NULL
)
""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
FORMAT AS json {}
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE','ARN'),
            config.get('S3', 'LOG_JSONPATH')       
            )

staging_songs_copy = ("""
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
FORMAT AS json 'auto'
""").format(config.get('S3',  'SONG_DATA'),
            config.get('IAM_ROLE','ARN')
           )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + ste.ts/1000 * INTERVAL '1 second' AS start_time,
       ste.userId     AS user_id,
       ste.level      AS level,
       sts.song_id    AS song_id,
       sts.artist_id  AS artist_id,
       ste.sessionId  AS session_id,
       ste.location   AS location,
       ste.userAgent  AS user_agent
FROM staging_events ste
JOIN staging_songs sts
ON (ste.artist = sts.artist_name)
AND ste.page = 'NextSong';
""")

########### INSERT INTO dimension table users
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT ste.userId  AS user_id,
                ste.firstName  AS first_name,
                ste.lastName   AS last_name,
                ste.gender     AS gender,
                ste.level      AS level
FROM staging_events ste
WHERE ste.page = 'NextSong';
""")

########## INSERT INTO dimension table songs
song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT sts.song_id    AS song_id,
                sts.title      AS title,
                sts.artist_id  AS artist_id,
                sts.year       AS year,
                sts.duration   AS duration
FROM staging_songs sts;
""")

########## INSERT INTO dimension table artists
artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT sts.artist_id         AS artist_id,
                sts.artist_name       AS name,
                sts.artist_location   AS location,
                sts.artist_latitude   AS latitude,
                sts.artist_longitude  AS longitude
FROM staging_songs sts;
""")

########## INSERT INTO dimension table time
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + ste.ts/1000 * INTERVAL '1 second' AS start_time,
                EXTRACT (hour    FROM start_time)   AS hour,
                EXTRACT (day     FROM start_time)   AS day,
                EXTRACT (week    FROM start_time)   AS week,
                EXTRACT (month   FROM start_time)   AS month,
                EXTRACT (year    FROM start_time)   AS year,
                EXTRACT (weekday FROM start_time) AS weekday
FROM staging_events ste
WHERE ste.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
