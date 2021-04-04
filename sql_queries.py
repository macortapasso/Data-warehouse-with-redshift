import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['arm']
LOG_DATA = config['S3']['log_data']
SONG_DATA = config['S3']['song_data']
LOG_JSONPATH = config['S3']['log_jsonpath']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                            CREATE TABLE IF NOT EXISTS staging_events (
                            artist TEXT,
                            auth TEXT,
                            first_name TEXT,
                            gender CHAR(1),
                            item_session INTEGER,
                            last_name TEXT,
                            length NUMERIC,
                            level TEXT,
                            location TEXT,
                            method TEXT,
                            page TEXT,
                            registration NUMERIC,
                            session_id INTEGER,
                            song TEXT,
                            status INTEGER,
                            ts BIGINT,
                            user_agent TEXT,
                            user_id INTEGER)
                            """)

staging_songs_table_create = ("""
                            CREATE  TABLE IF NOT EXISTS staging_songs (
                            num_songs INTEGER,
                            artist_id TEXT,
                            artist_latitude NUMERIC,
                            artist_longitude NUMERIC,
                            artist_location TEXT,
                            artist_name TEXT,
                            song_id TEXT,
                            title TEXT,
                            duration NUMERIC,
                            year INTEGER)
                            """)

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id INT IDENTITY(1,1) DISTKEY SORTKEY,
                            start_time TIMESTAMP,
                            user_id INTEGER NOT NULL,
                            level TEXT,
                            song_id TEXT,
                            artist_id TEXT,
                            session_id INTEGER,
                            location TEXT,
                            user_agent TEXT)
                            """)

user_table_create = ("""
                            CREATE TABLE IF NOT EXISTS users (
                            user_id INTEGER SORTKEY,
                            first_name TEXT NOT NULL,
                            last_name TEXT NOT NULL,
                            gender TEXT,
                            level TEXT)
                            """)

song_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songs (
                            song_id TEXT SORTKEY,
                            title TEXT,
                            artist_id TEXT,
                            year INTEGER,
                            duration NUMERIC )
                            """)

artist_table_create = ("""
                            CREATE TABLE IF NOT EXISTS artists (
                            artist_id TEXT SORTKEY,
                            name TEXT,
                            location TEXT,
                            latitude NUMERIC,
                            longitude NUMERIC)
                            """)

time_table_create = ("""
                            CREATE TABLE IF NOT EXISTS time (
                            start_time TIMESTAMP SORTKEY,
                            hour INTEGER,
                            day INTEGER,
                            week INTEGER,
                            month INTEGER,
                            year INTEGER,
                            weekDay INTEGER)
                            """)

# STAGING TABLES

staging_events_copy = ("""
                            COPY staging_events 
                            FROM {}
                            REGION 'us-west-2'
                            IAM_ROLE {}
                            JSON {}
                        """.format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)
                      )

staging_songs_copy = ("""
                            COPY staging_songs 
                            FROM {} 
                            REGION 'us-west-2'
                            IAM_ROLE {}
                            JSON 'auto'
                    """.format(SONG_DATA, IAM_ROLE)
                     )

# FINAL TABLES

songplay_table_insert = ("""
                        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                        SELECT  timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
                                e.user_id,
                                e.level,
                                s.song_id,
                                s.artist_id,
                                e.session_id,
                                e.location,
                                e.user_agent
                        FROM staging_events e
                        
                        INNER JOIN staging_songs s
                        ON e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration
                        
                        WHERE e.page = 'NextSong'            
""")

user_table_insert = ("""
                        INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT distinct  user_id,
                                first_name,
                                last_name,
                                gender,
                                level
                        FROM staging_events
                        WHERE page = 'NextSong'
                        """)

song_table_insert = ("""
                        INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT song_id, title, 
                                artist_id,
                                year,
                                duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
                        """)

artist_table_insert = ("""
                        INSERT INTO artists (artist_id, name, location, latitude, longitude)
                        SELECT distinct artist_id, 
                                artist_name,
                                artist_location,
                                artist_latitude,
                                artist_longitude 
                        FROM staging_songs
                        WHERE artist_id IS NOT NULL
                        """)

time_table_insert = ("""
                        INSERT INTO time (start_time, hour, day, week, month, year, weekDay)
                        SELECT start_time, 
                                EXTRACT(HOUR FROM start_time), 
                                EXTRACT(DAY FROM start_time),
                                EXTRACT(WEEK FROM start_time), 
                                EXTRACT(MONTH FROM start_time),
                                EXTRACT(YEAR FROM start_time), 
                                EXTRACT(DAYOFWEEK FROM start_time)
                        FROM songplays
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
