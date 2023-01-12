from datetime import datetime, timedelta
from airflow.models.dag import DAG 
from airflow.decorators import task

from lastfm.app.extract import extract_TopArtists  , extract_TopTracks
from lastfm.app.transform import transform_TopArtists, transform_TopTracks
from lastfm.app.load import load_data


#Se crean variables globales para usarlas en otra función (etl_top_artists())
global extract_TopArtists_
global transform_TopArtists_

extract_TopArtists_ = extract_TopArtists(total_pages=2, limit=5)
transform_TopArtists_ = transform_TopArtists(responses=extract_TopArtists_)


@task()
def etl_top_artists():
    load_data(df=transform_TopArtists_, table_dest='api_topartists')


@task()
def etl_top_tracks():
    extract_TopTracks_ = extract_TopTracks(df=transform_TopArtists_, limit=10)
    transform_Top_Tracks_ = transform_TopTracks(responses=extract_TopTracks_)
    load_data(df=transform_Top_Tracks_, table_dest='api_toptrack')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(dag_id="lastfm", default_args=default_args , schedule_interval="0 7 * * *", 
         start_date=datetime(2023, 1, 11), catchup=False,  tags=["API Last.fm"]) as dag:
       
    etl_top_artists() #>> etl_top_tracks()