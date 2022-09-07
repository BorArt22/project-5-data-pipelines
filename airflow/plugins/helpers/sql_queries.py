class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT stage.start_time,
               cast(extract(HOUR FROM stage.start_time) AS SMALLINT),
               cast(extract(DAY FROM stage.start_time) AS SMALLINT),
               cast(extract(WEEK FROM stage.start_time) AS SMALLINT),
               cast(extract(MONTH FROM stage.start_time) AS SMALLINT),
               cast(extract(YEAR FROM stage.start_time) AS SMALLINT),
               cast(to_char(stage.start_time, 'D') AS SMALLINT)
        FROM (SELECT distinct TIMESTAMP 'epoch' + staging_events.ts/1000 *INTERVAL '1 second' as start_time FROM staging_events) stage;
    """)