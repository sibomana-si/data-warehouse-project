import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# TABLE DROP QUERIES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# TABLE CREATE QUERIES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender CHAR,
        item_in_session INTEGER,
        last_name VARCHAR,
        length DECIMAL,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration DECIMAL,
        session_id INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration DECIMAL,
        year INTEGER
    );
""")

songplay_table_create = ("""
     CREATE TABLE IF NOT EXISTS song_plays (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time BIGINT NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER NOT NULL,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender CHAR,
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER,
        duration DECIMAL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,        
        latitude DECIMAL,
        longitude DECIMAL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    );
""")

songplay_table_add_references = ("""
    ALTER TABLE song_plays ADD CONSTRAINT FK_1 FOREIGN KEY (start_time) REFERENCES time (start_time);
    ALTER TABLE song_plays ADD CONSTRAINT FK_2 FOREIGN KEY (user_id) REFERENCES users (user_id);
    ALTER TABLE song_plays ADD CONSTRAINT FK_3 FOREIGN KEY (song_id) REFERENCES songs (song_id);
    ALTER TABLE song_plays ADD CONSTRAINT FK_4 FOREIGN KEY (artist_id) REFERENCES artists (artist_id);        
""")



# STAGING TABLES LOAD QUERIES

staging_events_copy = ("""
    copy staging_events
	from {}
	iam_role {}
	region 'us-west-2'
    format as json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs
	from {}
	iam_role {}
	region 'us-west-2'
    format as json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# ANALYTICS TABLES INSERT QUERIES

songplay_table_insert = ("""
    INSERT INTO song_plays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT staging_events.ts AS start_time, staging_events.user_id AS user_id, staging_events.level AS level, staging_songs.song_id AS song_id,
           staging_songs.artist_id AS artist_id, staging_events.session_id AS session_id, staging_events.location AS location, 
           staging_events.user_agent as user_agent
    FROM staging_events JOIN staging_songs 
    ON staging_events.artist = staging_songs.artist_name AND staging_events.song = staging_songs.title    
    WHERE staging_events.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users 
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs  
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs   
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists 
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time 
    SELECT event_ts as start_time,
        EXTRACT(hour FROM event_time) AS hour,
        EXTRACT(day FROM event_time) AS day,
        EXTRACT(week FROM event_time) AS week,
        EXTRACT(month FROM event_time) AS month,
        EXTRACT(year FROM event_time) AS year,
        EXTRACT(weekday FROM event_time) AS weekday
    FROM 
    (SELECT DISTINCT ts as event_ts, timestamp 'epoch' + (ts/1000) * interval '1 second' AS event_time FROM staging_events) AS t
""")


# QUERY LISTS

create_table_queries = [songplay_table_create, staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_add_references]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
