import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN=config.get("IAM_ROLE","ARN")
LOG_DATA=config.get("S3","LOG_DATA")
SONG_DATA = config.get("S3","SONG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists events 
(artist text,\
auth varchar(200),\
firstName varchar(200),\
gender varchar(200),\
itemInSession int,\
lastName varchar(200),\
length float,\
level varchar(200),\
location varchar(50),\
method varchar(50),\
page varchar(50),\
registration float,\
session_id int,\
song varchar(200),\
status int,\
ts bigint,\
userAgent text,\
userId int)
""")

staging_songs_table_create = ("""
Create table if not exists songs 
(
num_songs int,\
artist_id varchar(200) NOT NULL,\
artist_latitude DOUBLE PRECISION,\
artist_longitude DOUBLE PRECISION,\
artist_location varchar(2000),\
artist_name varchar(2000),\
song_id     varchar(200),\
title  varchar(200),\
duration DOUBLE PRECISION,\
year int,\
PRIMARY KEY (artist_id)
)


""")

songplay_table_create = ("""
create table if not exists songplays (
songplay_key int IDENTITY(0,1)  NOT NULL sortkey distkey,
start_time varchar(50),
song_id varchar(200),
artist_id varchar(200),
level varchar(50),
session_id int NOT NULL,
location varchar(50),
user_agent varchar(200),
user_id int NOT NULL,
PRIMARY KEY (songplay_key)
)
;
""")

user_table_create = ("""

CREATE TABLE if not exists users (\
user_key int IDENTITY(0,1) sortkey,\
userid int NOT NULL,\
firstName varchar(200),\
lastName varchar(200),\
gender varchar(200),\
level varchar(200),\
PRIMARY KEY (user_key)
);


""")

song_table_create = ("""
CREATE TABLE if not exists dim_songs 
(
song_key int IDENTITY(0,1) sortkey,\
song_id varchar(200) NOT NULL,\
title varchar(200),\
artist_id varchar(200),\
year int,\
duration DOUBLE PRECISION,\
PRIMARY KEY (song_key)
);

""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists(\
artist_key int IDENTITY(0,1) sortkey,\
artist_id varchar(200) NOT NULL,\
artist_name varchar(200),\
artist_location varchar(200),\
artist_latitude DOUBLE PRECISION,\
artist_longitude DOUBLE PRECISION\
)
diststyle all

""")

time_table_create = ("""
CREATE TABLE if not exists dim_time (\
date_key int IDENTITY(0,1) sortkey,\
start_time bigint NOT NULL,\
hour int ,\
day int ,\
month int,\
year int ,\
dayofweek int,\
PRIMARY KEY (date_key) 
)
diststyle all;

""")

# STAGING TABLES

staging_events_copy = (""" 
    copy events from {}
    credentials 'aws_iam_role={}'
    FORMAT AS json {};
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
    copy songs from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    compupdate off region 'us-west-2';

""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays 
(artist_id,user_id,start_time,song_id,level,session_id,location,user_agent)
select 
artist_id,
userId,
ts as start_time,
song_id,
level,
session_id,
location,
userAgent
from events e
join songs s
on e.song = s.title
where e.page = 'NextSong'

""")

user_table_insert = ("""
insert into users (userid,firstName,lastName,gender,level) 
SELECT
userid,
firstName,
lastName,
gender,
level
from (
        select
        userid,
        firstName,
        lastName,
        gender,
        level,
        from_unixtime(ts) as date,
        row_number() OVER ( PARTITION BY userid ORDER BY date DESC) as row_number
        from  events 
    )
where userid is not null
and row_number = 1
;
""")

song_table_insert = ("""
insert into dim_songs (song_id,title,artist_id,year,duration)
select 
DISTINCT song_id,
title,
artist_id,
year,
duration
from songs
""")

artist_table_insert = ("""
INSERT INTO dim_artists 
(artist_id,artist_name,artist_location,artist_latitude,artist_longitude)
select DISTINCT artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
from songs
""")

time_table_insert = ("""
INSERT INTO dim_time
(start_time,hour,day,month,year,dayofweek)
select 
ts,
EXtract(hour from mytimestamp),
EXtract(day from mytimestamp),
EXtract(month from mytimestamp),
EXtract(year from mytimestamp),
EXtract(dow from mytimestamp)

from (
    SELECT ts,(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as mytimestamp
from events 
)
""")

#function 


time_converter = ("""
CREATE OR REPLACE FUNCTION from_unixtime(epoch BIGINT)\
  RETURNS TIMESTAMP  AS\
'from datetime import datetime\

x = lambda epoch : datetime.fromtimestamp(epoch/1000.0)\

return x(epoch)\

'\
LANGUAGE plpythonu IMMUTABLE;""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

