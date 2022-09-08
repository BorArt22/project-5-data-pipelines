class SqlQueries:
    songplay_table_insert = ("""
        CREATE TEMP TABLE temp_table_songplays (like public.songplays);
        INSERT INTO temp_table_songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
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
        ;
        BEGIN TRANSACTION;
            DELETE FROM temp_table_songplays
            USING public.songplays
            WHERE temp_table_songplays.songplay_id = songplays.songplay_id
            ;
            INSERT INTO public.songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
            SELECT *
            FROM temp_table_songplays
            ;
        END TRANSACTION;
        DROP TABLE temp_table_songplays;
    """)

    user_table_insert = ("""
        CREATE TEMP TABLE temp_table_users (like public.users);
        INSERT INTO temp_table_users (user_id, first_name, last_name, gender, level)
        SELECT 
            dedubl.userid,
            dedubl.firstname,
            dedubl.lastname,
            dedubl.gender,
            dedubl.level
        FROM (   
            SELECT 
                stage.userid,
                stage.firstname,
                stage.lastname,
                stage.gender,
                stage.level,
                row_number() over (partition by stage.userid order by stage.ts desc) as rn
            FROM staging_events stage
            WHERE page='NextSong' AND stage.userid is not null   
        ) dedubl
        WHERE rn = 1
        ;
        BEGIN TRANSACTION;
            UPDATE public.users
            SET level = stage.level
            FROM temp_table_users stage
            WHERE users.userid = stage.userid
            ;
            DELETE FROM temp_table_users 
            USING public.users 
            WHERE temp_table_users.userid = users.userid
            ; 
            INSERT INTO public.users
            (SELECT userid, firstname, lastname, gender, level
            FROM temp_table_users)
            ;
        END TRANSACTION;
        DROP TABLE temp_table_users; 
    """)

    song_table_insert = ("""
        CREATE TEMP TABLE temp_table_songs (like public.songs);
        INSERT INTO temp_table_songs (song_id, title, artist_id, year, duration)
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
                ) dedubl
            WHERE rn = 1;
        BEGIN TRANSACTION;
            DELETE FROM temp_table_songs
            USING public.songs
            WHERE temp_table_songs.song_id = songs.song_id
            ;
            INSERT INTO public.songs (song_id, title, artist_id, year, duration)
            SELECT *
            FROM temp_table_songs
            ;
        END TRANSACTION;
        DROP TABLE temp_table_songs;
    """)

    artist_table_insert = ("""
        CREATE TEMP TABLE temp_table_artists (like public.artists);
        INSERT INTO temp_table_artists (artist_id, name, location, latitude, longitude)
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
            ) dedubl
        WHERE rn = 1   
        ;
        BEGIN TRANSACTION;
            DELETE FROM temp_table_artists 
            USING public.artists
            WHERE temp_table_artists.artist_id = artists.artist_id
            ;
            INSERT INTO public.artists (artist_id, name, location, latitude, longitude)
            SELECT *
            FROM temp_table_artists
            ;
        END TRANSACTION;
        DROP TABLE temp_table_artists;
    """)

    time_table_insert = ("""
        CREATE TEMP TABLE temp_table_time (start_time timestamp NOT NULL);
        INSERT INTO temp_table_time
        SELECT distinct TIMESTAMP 'epoch' + staging_events.ts/1000 *INTERVAL '1 second' as start_time 
        FROM staging_events
        WHERE page='NextSong' 
        ;
        BEGIN TRANSACTION;
            DELETE FROM temp_table_time 
            USING public."time" 
            WHERE temp_table_time.start_time = time.start_time
            ; 
            INSERT INTO public."time" (start_time, hour, day, week, month, year, weekday)
            SELECT 
                stage.start_time,
                cast(extract(HOUR FROM stage.start_time) AS int4),
                cast(extract(DAY FROM stage.start_time) AS int4),
                cast(extract(WEEK FROM stage.start_time) AS int4),
                cast(extract(MONTH FROM stage.start_time) AS int4),
                cast(extract(YEAR FROM stage.start_time) AS int4),
                cast(to_char(stage.start_time, 'D') AS int4)
            FROM temp_table_time stage;
        END TRANSACTION;
        DROP TABLE temp_table_time;
    """)