class SqlQueries:
    """
    contains SQL Queries for upsert operations in datapipeline
    """
    songplay_table_insert = ("""
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            artists.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, staging_events.*
              FROM staging_events
              WHERE page='NextSong') events
        LEFT JOIN songs ON upper(BTRIM(songs.title)) = upper(BTRIM(events.song))
                       AND trunc(songs.duration) = trunc(events.length)
        LEFT JOIN artists ON upper(BTRIM(artists.name)) = upper(BTRIM(events.artist))
        WHERE NOT EXISTS (SELECT songplay_id FROM songplays WHERE songplays.songplay_id =  md5(events.sessionid || events.start_time))
    """)

    songplay_table_key = ("songplay_id")

    songplay_table_fields = ("songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent")


    user_table_insert = ("""
        SELECT 
            stage.user_id,
            stage.firstname,
            stage.lastname,
            stage.gender,
            stage.level
        FROM (   
            SELECT 
                stage.userid as user_id,
                stage.firstname,
                stage.lastname,
                stage.gender,
                stage.level,
                row_number() over (partition by stage.userid order by stage.ts desc) as rn
            FROM staging_events stage
            WHERE page='NextSong' AND stage.userid is not null
        ) stage
        WHERE rn = 1  AND {INSERT_MODE_QUERY} 
    """)

    user_table_key = ("user_id")

    user_table_fields = ("user_id, first_name, last_name, gender, level")

    song_table_insert = ("""
            SELECT
                dedubl.song_id,
                dedubl.title,
                dedubl.artist_id,
                dedubl.year,
                dedubl.duration
            FROM (
                SELECT 
                    stage.song_id,
                    stage.title,
                    stage.artist_id,
                    stage.year,
                    stage.duration,
                    row_number() over (partition by stage.song_id order by LEN(stage.title) asc, stage.title asc) as rn
                FROM staging_songs stage
                WHERE {INSERT_MODE_QUERY}
                ) dedubl
            WHERE rn = 1
    """)

    song_table_key = ("song_id")

    song_table_fields = ("song_id, title, artist_id, year, duration")

    artist_table_insert = ("""
        SELECT 
        dedubl.artist_id,
        dedubl.artist_name,
        dedubl.artist_location,
        dedubl.artist_latitude,
        dedubl.artist_longitude
        FROM (
            SELECT 
                stage.artist_id,
                stage.artist_name,
                stage.artist_location,
                stage.artist_latitude,
                stage.artist_longitude,
                row_number() over (partition by stage.artist_id order by LEN(stage.artist_name) asc, stage.artist_name asc) as rn
            FROM staging_songs stage
            WHERE {INSERT_MODE_QUERY}
            ) dedubl
        WHERE rn = 1
    """)

    artist_table_key = ("artist_id")

    artist_table_fields = ("artist_id, name, location, latitude, longitude")

    time_table_insert = ("""
        SELECT 
            stage.start_time,
            cast(extract(HOUR FROM stage.start_time) AS int4),
            cast(extract(DAY FROM stage.start_time) AS int4),
            cast(extract(WEEK FROM stage.start_time) AS int4),
            cast(extract(MONTH FROM stage.start_time) AS int4),
            cast(extract(YEAR FROM stage.start_time) AS int4),
            cast(to_char(stage.start_time, 'D') AS int4)
        FROM (
            SELECT
                distinct TIMESTAMP 'epoch' + staging_events.ts/1000 *INTERVAL '1 second' as start_time 
            FROM staging_events
            WHERE page='NextSong') stage
        WHERE {INSERT_MODE_QUERY}
    """)

    time_table_key = ("start_time")

    time_table_fields = ("start_time, hour, day, week, month, year, weekday")