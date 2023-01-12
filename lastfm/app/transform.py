import pandas as pd
from lastfm.app.extract import lookup_tags
from tqdm import tqdm 
tqdm.pandas() 

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

        #print()
        #print('Obteniendo tags de artistas')
        TopArtists_f['tags'] = TopArtists_f['name'].progress_apply(lookup_tags)
        
        TopArtists_f.to_csv('artists.csv', index=False, sep=';')
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