import requests
url1 = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv'
url2 = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv'
url3 = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv'
museum = requests.get(url1, allow_redirects=True)
cinema = requests.get(url2, allow_redirects=True)
library = requests.get(url3, allow_redirects=True)

# cambiar el path a lo que recomienda el pdf? preguntar
open('museum.csv', 'wb').write(museum.content)
open('cinema.csv', 'wb').write(cinema.content)
open('library.csv', 'wb').write(library.content)

from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://postgres:pass@localhost:5432/Alkemy', echo=True)

import pandas as pd
df_museum = pd.read_csv('museum.csv', sep=',', encoding='UTF-8')
df_museum = df_museum[['Cod_Loc','IdProvincia','IdDepartamento','categoria','provincia','localidad',
                       'nombre','direccion','CP','telefono','Mail','Web']]
df_museum.rename(columns = {'categoria':'Categoría', 'provincia':'Provincia', 'localidad':'Localidad',
                            'nombre':'Nombre', 'direccion':'Dirección', 'telefono':'Teléfono'}, inplace = True)
df_cinema = pd.read_csv('cinema.csv', sep=',', encoding='UTF-8')
df_cinema = df_cinema[['Cod_Loc','IdProvincia','IdDepartamento','Categoría','Provincia','Localidad',
                       'Nombre','Dirección','CP','Teléfono','Mail','Web']]
df_library = pd.read_csv('library.csv', sep=',', encoding='UTF-8')
df_library = df_library[['Cod_Loc','IdProvincia','IdDepartamento','Categoría','Provincia','Localidad',
                       'Nombre','Domicilio','CP','Teléfono','Mail','Web']]
df_library.rename(columns = {'Domicilio':'Dirección'}, inplace = True)

df_main = df_museum.append(df_cinema)
df_main = df_main.append(df_library)

from sqlalchemy.types import Integer, Text
df_main.to_sql(
    'main_table',
    engine,
    if_exists='replace',
    index=False,
    chunksize=500,
    dtype={
        "Cod_Localidad": Integer,
        "IdProvincia": Integer,
        "IdDepartamento": Integer,
        "Categoría":  Text,
        "Provincia": Text,
        "Localidad": Text,
        "Nombre": Text,
        "Dirección": Text,
        "CP": Text,
        "Teléfono": Text,
        "Mail": Text,
        "Web": Text
        })