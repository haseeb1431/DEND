# DROP TABLES

songplay_table_drop = "DROP table if exists songplays"
user_table_drop = "DROP table if exists  users"
song_table_drop = "DROP table if exists songs"
artist_table_drop = "DROP table if exists  artists"
time_table_drop = "DROP table if exists time"

# CREATE TABLES

songplay_table_create = (""" 
  create table songplays (
        songplay_id serial primary key,
        start_time timestamp not null,
        user_id int not null,
        level varchar not null,
        song_id char (18),
        artist_id char (18),
        session_id int not null,
        location varchar,
        user_agent varchar not null
    )
""")

user_table_create = ("""
create table users (
        user_id int primary key,
        first_name varchar not null,
        last_name varchar not null,
        gender char (1) not null,
        level varchar not null
    )
""")

song_table_create = ("""
    create table songs (
        song_id char (18) primary key,
        title varchar not null,
        artist_id char (18) not null,
        year int not null,
        duration numeric not null
    )
""")

artist_table_create = ("""
    create table artists (
        artist_id char (18) primary key,
        name varchar not null,
        location varchar,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    create table time (
        start_time timestamp primary key,
        hour int not null,
        day int not null,
        week int not null,
        month int not null,
        year int not null,
        weekday int not null
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (user_id) DO UPDATE SET first_name=users.first_name, last_name=users.last_name, gender=users.gender, level=users.level """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO UPDATE SET title=songs.title, artist_id=songs.artist_id,
year=songs.year, duration=songs.duration """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE SET name=artists.name, location=artists.location, latitude=artists.latitude, 
longitude=artists.longitude """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO UPDATE SET hour=time.hour, day=time.day, week=time.week, month=time.month, 
year=time.year, weekday=time.weekday """)

# FIND SONGS

# FIND SONGS BY SONG_ID AND ARTIST_ID
song_select_by_song_id_artist_id = ("""SELECT s.song_id, a.artist_id FROM songs s, artists a
WHERE s.artist_id = a.artist_id  
    AND s.title = %s
    AND a.name = %s
    AND s.duration = %s
""")

# FIND SONG BY ID
song_select = ("""SELECT COUNT(*) FROM songs s
WHERE s.song_id = %s
""")

# FIND ARTIST BY ID
artist_select = ("""SELECT COUNT(*) FROM artists a
WHERE a.artist_id = %s
""")

# FIND USER BY ID
user_select = ("""SELECT COUNT(*) FROM users u
WHERE u.user_id = %s
""")

# FIND TIME BY ID
time_select = ("""SELECT COUNT(*) FROM time t
WHERE t.start_time = %s
""")
# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]