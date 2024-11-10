import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('/Users/andrekrasinski/Documents/GitHub/DataEngineerAWS/dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create=(
""" 
    create table if not exists staging_events (
        artist         varchar  null,
        auth           varchar  null,
        firstName      varchar  null,
        gender         varchar  null,
        itemInSession  integer  null,
        lastName       varchar  null,
        length         float    null,
        level          varchar  null,
        location       varchar  null,
        method         varchar  null,
        page           varchar  null,
        registration   float    null,
        sessionId      integer  null,
        song           varchar  null,
        status         integer  null,
        ts             bigint   null,
        userAgent      varchar  null,
        userId         integer  null
    )

"""
)

staging_songs_table_create = (
"""
    create table if not exists staging_songs (
        num_songs        integer       null,
        artist_id        varchar       null,
        artist_latitude  float         null,
        artist_longitude float         null,
        artist_location  varchar(2048) null,
        artist_name      varchar(2048) null,
        song_id          varchar       null,
        title            varchar(2048) null,
        duration         float         null,
        year             integer       null
        )
     
"""
)

songplay_table_create = (
"""
    create table if not exists songplays (
        songplay_id   integer identity(0,1) not null primary key, 
        start_time    timestamp not null, 
        user_id       integer not null references users(user_id), 
        level         varchar not null, 
        song_id       varchar references songs(song_id),
        artist_id     varchar references artists(artist_id),
        session_id    integer not null,
        location      varchar,
        user_agent    varchar
    )   
"""
)

user_table_create = (
"""
    create table if not exists users (
        user_id     integer not null primary key, 
        first_name  varchar, 
        last_name   varchar, 
        gender      varchar, 
        level       varchar not null
    )
""")

song_table_create = (
"""
    create table if not exists songs (
        song_id     varchar not null primary key, 
        title       varchar(2048) not null, 
        artist_id   varchar references artists(artist_id), 
        year        integer, 
        duration    float
    )
"""
)

artist_table_create = (
"""
    create table if not exists artists (
        artist_id   varchar not null primary key, 
        name        varchar(2048) not null,  
        location    varchar(2048), 
        latitude    float,
        longitude   float
    )
"""
)

time_table_create = (
"""
    create table if not exists time (
        start_time  timestamp not null, 
        hour        integer not null, 
        day         integer not null, 
        week        integer not null, 
        month       integer not null,
        year        integer not null,
        weekday     integer not null
    )
"""
)

# STAGING TABLES

staging_events_copy =(
"""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {}
    region 'us-west-2';   
"""
).format(
    config.get("DWH","LOG_DATA"),
    config.get("DWH","DWH_ROLE_ARN"),
    config.get("DWH","LOG_JSONPATH")
)

staging_songs_copy = (
"""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';   
"""
).format(
    config.get("DWH","SONG_DATA"),
    config.get("DWH","DWH_ROLE_ARN")
)

# FINAL TABLES

songplay_table_insert = (
"""
    insert into songplays (
        start_time, 
        user_id, 
        level, 
        song_id,
        artist_id,
        session_id,
        location,
        user_agent   
    )
    select
        timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
        e.userId as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId as session_id,
        e.location, 
        e.userAgent as user_agent  
    from 
        staging_events e
    left join 
        staging_songs s 
    on 
        trim(lower(e.song)) = trim(lower(s.title))
    and 
        trim(lower(e.artist)) = trim(lower(s.artist_name))
    where 
        e.page = 'NextSong'
"""
)

user_table_insert = (
"""
    insert into users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    )
    select distinct
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    from 
        staging_events
    where userId is not null
"""
)

song_table_insert = (
"""
    insert into songs (
        song_id, 
        title,
        artist_id,  
        year, 
        duration
    )
    select distinct
        song_id, 
        title,
        artist_id, 
        year, 
        duration
    from 
        staging_songs
    where song_id is not null
"""
)

artist_table_insert = (
"""
    insert into artists (
        artist_id, 
        name,  
        location, 
        latitude,
        longitude
    )
    select distinct
        artist_id, 
        artist_name as name, 
        artist_location  as location, 
        artist_latitude  as latitude,
        artist_longitude as longitude 
    from 
        staging_songs
    where artist_id is not null
"""
)

time_table_insert = (
"""
    insert into time(
        start_time, 
        hour, 
        day, 
        week, 
        month,
        year,
        weekday
    )
    select
        timestamp 'epoch' +ts/1000 * interval '1 second' as start_time,
        extract(hour from start_time)     as hour,
        extract(day from start_time)      as day,
        extract(week from start_time)     as week,
        extract(month from start_time)    as month,
        extract(year from start_time)     as year,
        extract(weekday from start_time)  as weekday
    from 
        staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, artist_table_create, song_table_create, user_table_create,time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
