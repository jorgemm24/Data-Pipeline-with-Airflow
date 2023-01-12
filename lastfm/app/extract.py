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
    
    response = lastfm_get({'method':'artist.getTopTags', 'artist': artist})
    
    if response.status_code !=200:
        return None
    
    tags = [t['name']  for t in response.json()['toptags']['tag'][:3]]
    tags_str = ', '.join(tags)

    time.sleep(1.5)
    return tags_str