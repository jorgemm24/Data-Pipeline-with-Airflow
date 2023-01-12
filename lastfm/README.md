### Resumen Proyecto Last.fm:

Este proyecto tiene como finalidad realizar un data pipeline utilizando Airflow. 

### Arquitectura:
por implementar

### Herramientas utilizadas:
- Docker, Airflow, Python, API rest, VSCODE, PostgreSQL, DBeaver, Last.fm, WSL


### API Last.fm:

El proceso extrae información del API Last.fm el cual es una plataforma de musica.

Documentación del API: https://www.last.fm/api
Web de Last.fm:  https://www.last.fm/

Se usan los siguientes metodos del API:  

- 'chart.getTopArtists' -> retorna la los artistas mas populares, para este proyecto se 		   obtendra 100 artistas
- 'artist.getTopTags'   -> retorna el tag de cada artista, en este proyecto solo se obtuvo 3 tags por artita
- 'artist.getTopTracks' -> retorna el top de canciones del cada artista, para el proyecto se obtuvo el top 10 de canciones.

Para poder usar el API, se tiene que crear una cuenta y se obtendra un API_KEY, leer la documentación para no 
sobrepasar el numero de solicitudes y evitar un posible baneo.

Se utiliza el archivo .env para guardar las credenciales

### Airflow:

- Para este proyecto se instalo airflow sobre Docker (plataforma windows).
- Por el momento se esta usando lo basico de airflow. Se uso DAG, task, taskGroup.
- Se orquesto y programo el dag para que se ejecute en una determinada hora.

### Python: 
Para procesar la información se usara Python modularmente usando paquetes.

### Explicación del codigo:

##### extract.py

```python
import requests
import os
import time
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm 

load_dotenv()
tqdm.pandas() 


def lastfm_get(payload: dict) -> requests.get:
    """
    Función para conectarse a la Api de Last.fm
    
    Args:
        payload (dict): Se puede asignar parametros (diccionario) dinamicamente para conectarse a los 
                        diferentes metodos del API.

    Returns:
        requests.Response: Retorna un objecto request.get del metodo que se asigno en el argumento. 
    """
    try:
        API_KEY = os.getenv('API_KEY')
        USER_AGENT = os.getenv('USER_AGENT')

        headers = {'user-agent':USER_AGENT}
        
        payload['api_key'] = API_KEY
        payload['format']  = 'json'
        
        url = 'https://ws.audioscrobbler.com/2.0/'
        response = requests.get(url, headers=headers, params=payload) 
        
    except Exception as e:
        print(f'Error durante la conexión al API: {e}')
        
    return response


def extract_TopArtists(total_pages: int, limit: int) -> list:
    """
    Función para extraer el metodo 'char.gettopartists' del API Last.fm

    Args:
        total_pages (int): Total de paginas que se recorrera del API
        limit (int): Cantidad de registros por pagina del API

    Returns:
        list: Retorna una lista "responses" con los (request.get) del metodo 'char.gettopartists' 
    """
    responses = []

    try:
        page = 1
        total_pages = total_pages

        while page <= total_pages:

            payload = {'method':'chart.getTopArtists', 'limit': limit, 'page': page}
            
            print(f"Pagina de solicitud: {page}/{total_pages}")
            response = lastfm_get(payload)
            
            if response.status_code != 200:
                print(response.text)
                break
            
            responses.append(response)
        
            page += 1
            time.sleep(1)
            #os.system('clear')  # clean terminal

    except Exception as e:
        print(f'Error durante el proceso de extración {e}')
    
    return responses


def extract_TopTracks(df: pd.DataFrame, limit: int) -> list:   
    """Esta función tiene como finalidad extraer el metodo "artist.getTopTracks" del API Last.fm
       Primero se necesita obtener el Dataframe de TopArstists del metodo "chart.getTopArtists" para obtener los "nombres"
       para poder recorrer el metodo

    Args:
        df (pd.DataFrame): Dataframe de "chart.getTopArtists"
        limit (int): cantidad de canciones de cada artista (nombre)

    Returns:
        list: Retorna una lista con los "responses" del metodo "artist.getTopTracks"
    """
    
    lista_respones_TopTrack = []
    payload = {'method':'artist.getTopTracks', 'limit':limit}
    
    print()
    for index, row in df.iterrows():
        print(f"{index}. Obteniedo tracks de: {row['name']}")
        
        payload['artist'] = row['name']
        response = lastfm_get(payload)
        
        if response.status_code != 200:
            return None
        
        time.sleep(1)
        lista_respones_TopTrack.append(response)
        #os.system('clear')   # clean terminal
        
    return lista_respones_TopTrack


def lookup_tags(artist: str) -> str:
    """
    Función para obtener el tag de cada artista, solo se obtiene 3 los primeros tags
    del metodo "artist.getTopTags" del API Last.fm
    
    Args:
        artist (str): Se recibe el nombre del artista para obtener sus tags

    Returns:
        str: Retorna una cadena de 3 tags delimitados por ','
    """
    
    #print("Iniciando la Función 'lookup_tags'")
    response = lastfm_get({'method':'artist.getTopTags', 'artist': artist})
    
    if response.status_code !=200:
        return None
    
    tags = [t['name']  for t in response.json()['toptags']['tag'][:3]]
    tags_str = ', '.join(tags)

    time.sleep(1)
    return tags_str
```

##### transform.py
```python
import pandas as pd
from lastfm.app.extract import lookup_tags


def transform_TopArtists(responses: list) -> pd.DataFrame:  
    """Función para transformar la lista de responses a un dataframe listo
       Primero se llama a la función "getTopArtis" el cual traera el "request.get" para iniciar con la transformación,
       luego se realizara las transformaciónes necesarias.

    Args:
        responses (list): Recibe una variable de la función "extract_TopArtists"

    Returns:
        pd.DataFrame: Retorna un dataframe transformado
    """
    try:
        frames = [pd.DataFrame(r.json()['artists']['artist'])  for r in responses]
        TopArtists = pd.concat(frames)
        
        print()
        print('Limpiando registros')
        print(f'Registros con duplicados -> {len(TopArtists)}')
        TopArtists_f = TopArtists.drop_duplicates(subset=['name'])
        print(f'Registros sin duplicados -> {len(TopArtists_f)}')
        TopArtists_f = TopArtists_f.drop('image', axis=1) # drop column "image"
        TopArtists_f[['playcount','listeners','streamable']] = TopArtists_f[['playcount','listeners','streamable']].astype('int64')

        print()
        print('Obteniendo tags de artistas')
        TopArtists_f['tags'] = TopArtists_f['name'].progress_apply(lookup_tags)
        
        #TopArtists_f.to_csv('artists.csv', index=False, sep=';')
        #print(TopArtists_f.head())
        #print(TopArtists_f.dtypes)

    except Exception as e:
        print(f'Error durante el proceso de transformación: {e}')
    
    return TopArtists_f


def transform_TopTracks(responses: list) -> pd.DataFrame:
    """
    Función para transformar los request.get del metodo "artist.getTopTracks", se extrae los campos necesarios 
    de acuerdo a nuestra necesidad

    Args:
        responses_TopTracks (lsit): recibe la lista del metodo "getTopTracks" el cual retorna una lista con request.get
    
    Returns:
        Retorna un dataframe con el top track de cada artista
    """
    
    print()
    print("Iniciando Función -> transform_getTopTracks")
    try:
        lista_datos = []
    
        for r in responses:        
            for doc in r.json()['toptracks']['track']:
                datos = {}

                datos['name_track'] = doc['name']
                datos['playcount_track'] = doc['playcount']
                datos['listeners_track'] = doc['listeners']
                datos['url_track'] = doc['url']
                datos['name_artist'] = doc['artist']['name']
                try:
                    datos['mbid_artist'] = str(doc['artist']['mbid'])
                except:
                    datos['mbid_artist'] = None
                datos['url_artist'] = doc['artist']['url']
                datos['rank'] = doc['@attr']['rank']
                
                lista_datos.append(datos)
                   
        print()
        df_topTrack = pd.DataFrame(lista_datos)
        df_topTrack[['playcount_track','listeners_track','rank']] = df_topTrack[['playcount_track','listeners_track','rank']].astype('int64')
        
        #df_topTrack.to_csv('topTrack.csv', index=False, sep=';')
        #print(df_topTrack.dtypes)
        #print(df_topTrack)
        
    except Exception as e:
        print(f'Error durante el proceso de transformación: {e}')
    
    return df_topTrack
````

##### load.py
```python
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

load_dotenv()


def conn_postgres_alchemy() -> create_engine:
    """
    Función para conectarse a la base de datos destiono (PostgreSQL)

    Returns:
        create_engine: Retorna el motor de conexión a PostgreSQL
    """
    try:
        user = os.getenv('PGUSER')
        password=  os.getenv('PGPASSWORD')
        host = os.getenv('PGHOST')
        schema = os.getenv('PGDATABASE')
        port = os.getenv('PGPORT')
        
        # sqlquery = "select * from API_topartists limit 10;"
        # print(create_engine.execute(sqlquery))
        engine_postgres = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{schema}")
    except Exception as e:
        print(f'Error durante conección a PostgreSQL: {e}')
    return engine_postgres


def load_data(df: pd.DataFrame, table_dest: str):
    """
    Función para cargar el Dataframe a la base de datos destino PostgreSQL, la función
    no retorna ningun valor

    Args:
        df (pd.DataFrame): Dataframe listo para cargar
        table_dest (str): Tabla destino donde se cargara el dataframe
    """
    engine_postgres = conn_postgres_alchemy()
    try:
        #trans = engine_postgres.begin()
        with engine_postgres.begin() as trans:
            df.to_sql(table_dest,  engine_postgres, if_exists='append', index=False, chunksize=50)
            print('Data cargada')
        
    except Exception as e:
        trans.rollback()
        print(f'Erro durante la carga: {e}')
```


### lastfm.py (MAIN)
```python
from datetime import datetime, timedelta
from airflow.models.dag import DAG 
from airflow.decorators import task

from lastfm.app.extract import extract_TopArtists  , extract_TopTracks
from lastfm.app.transform import transform_TopArtists, transform_TopTracks
from lastfm.app.load import load_data




#Se crean variables globales para usarlas en otra función (etl_top_artists())
global extract_TopArtists_
global transform_TopArtists_

extract_TopArtists_ = extract_TopArtists(total_pages=2, limit=2)
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
       
    etl_top_artists() >> etl_top_tracks()
```

### Capturas del proyecto:

##### Docker
[![5-docker.png](https://i.postimg.cc/8Phd9GM6/5-docker.png)](https://postimg.cc/svgZGtnj)


##### airflow panel principal
[![1-airflow.png](https://i.postimg.cc/903HwLw7/1-airflow.png)](https://postimg.cc/phB6gYZW)

##### proyecto Last.fm
[![2-airflow.png](https://i.postimg.cc/MGNptbjW/2-airflow.png)](https://postimg.cc/hJ8ntmrw)

##### DAG ejecutado correctamente
[![4-airflow.png](https://i.postimg.cc/MGDYQHcb/4-airflow.png)](https://postimg.cc/v149RQtc)


##### Base de Datos Destino (PostgreSQL)

##### api -> method :chart.getTopArtists y method : artist.getTopTags
[![6-db-postgres.png](https://i.postimg.cc/9XgYZ1VD/6-db-postgres.png)](https://postimg.cc/nM7mt4Xx)

##### api -> method: artist.getTopTracks
[![7-db-postgres.png](https://i.postimg.cc/9fLwt7cb/7-db-postgres.png)](https://postimg.cc/KKgYm4k3)


### Contacto:
ztejorge@hotmail.com
