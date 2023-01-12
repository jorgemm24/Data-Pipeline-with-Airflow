-- drop table public.api_topartists;
-- drop table public.api_toptrack;

/**/


create table public.api_topartists (
name          varchar(50),
playcount     float,
listeners     float,
mbid          varchar(50),
url           varchar(50),
streamable    smallint,
tags          varchar(150)
);


create table public.api_toptrack (
name_track         varchar(100),
playcount_track    float,
listeners_track    float,
url_track          varchar(150),
name_artist        varchar(100),
mbid_artist        varchar(50),
url_artist         varchar(100),
rank               smallint

);

-- truncate table public.API_topartists;
-- truncate table public.API_topTrack;

select count(1) from public.API_topartists;
select count(1) from public.API_topTrack;

select * from public.api_topartists limit 100;
select * from public.api_toptrack limit 100;