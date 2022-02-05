import logging
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Integer, Text

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s - %(message)s')
current_datetime = datetime.now().strftime("%d-%m-%Y")
current_datetime = str(current_datetime)

logging.info('Generando los CVS')
url1 = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv'
url2 = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv'
url3 = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv'
museum = requests.get(url1, allow_redirects=True)
cinema = requests.get(url2, allow_redirects=True)
library = requests.get(url3, allow_redirects=True)
open('museos_'+current_datetime+'.csv', 'wb').write(museum.content)
open('cines_'+current_datetime+'.csv', 'wb').write(cinema.content)
open('bibliotecas_'+current_datetime+'.csv', 'wb').write(library.content)

logging.info('Conectando con PostgreSQL')
# 'postgres+psycopg2://<USERNAME>:<PASSWORD>@<IP_ADDRESS>:<PORT>/<DATABASE_NAME>'
engine = create_engine('postgresql+psycopg2://postgres:pass@localhost:5432/Alkemy')

logging.info('Normalizando la data')
df_museum = pd.read_csv('museos_'+current_datetime+'.csv', sep=',', encoding='UTF-8')
df_museum.rename(columns = {'categoria':'Categoría', 'provincia':'Provincia', 'localidad':'Localidad',
                            'nombre':'Nombre', 'direccion':'Dirección', 'telefono':'Teléfono',
                            'fuente':'Fuente'}, inplace = True)
df_cinema = pd.read_csv('cines_'+current_datetime+'.csv', sep=',', encoding='UTF-8')
df_library = pd.read_csv('bibliotecas_'+current_datetime+'.csv', sep=',', encoding='UTF-8')
df_library.rename(columns = {'Domicilio':'Dirección'}, inplace = True)

# dataframe principal
master_df = df_museum.append(df_cinema)
master_df = master_df.append(df_library)

logging.info('Creando las tablas')
# tabla 1
table1 = master_df.loc[:,['Cod_Loc','IdProvincia','IdDepartamento','Categoría','Provincia','Localidad',
                       'Nombre','Dirección','CP','Teléfono','Mail','Web']]
table1['Fecha de carga'] = pd.to_datetime('today').strftime("%d-%m-%Y")

# tabla 2
table2_1 = master_df.groupby(['Categoría']).size().to_frame(name = 'Total por categoría')
table2_2 = master_df.groupby(['Categoría','Fuente']).size().to_frame(name = 'Total por fuente')
table2_3 = master_df.groupby(['Categoría','Provincia']).size().to_frame(name = 'Categorías por provincia')
table2 = table2_1.merge(table2_2, how='outer', left_index=True, right_index=True)
table2 = table2.merge(table2_3, how='outer', left_index=True, right_index=True)
table2.reset_index(inplace=True)
table2.set_index('Categoría', inplace=True)
table2 = table2[['Total por categoría','Fuente','Total por fuente','Provincia','Categorías por provincia']]
table2['Fecha de carga'] = pd.to_datetime('today').strftime("%d-%m-%Y")

# tabla 3
table3 = df_cinema.loc[:,['Provincia','Pantallas','Butacas','espacio_INCAA']]
aggregation_functions = {'Pantallas': 'sum', 'Butacas': 'sum','espacio_INCAA':'count'}
table3 = table3.groupby(table3['Provincia']).aggregate(aggregation_functions)
table3['Fecha de carga'] = pd.to_datetime('today').strftime("%d-%m-%Y")

# postgres
logging.info('Subiendo tabla 1 a PostgreSQL: main_table')
table1.to_sql(
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
logging.info('Subiendo tabla 2 a PostgreSQL: category_table')
table2.to_sql(
    'category_table',
    engine,
    if_exists='replace',
    chunksize=500,
    dtype={
        "Total por categoría": Integer,
        "Fuente": Text,
        "Total por fuente": Integer,
        "Provincia":  Text,
        "Categorías por provincia": Integer,
        })
logging.info('Subiendo tabla 3 a PostgreSQL: cinema_table')
table3.to_sql(
    'cinema_table',
    engine,
    if_exists='replace',
    chunksize=500,
    dtype={
        "Provincia": Text,
        "Pantallas": Integer,
        "Butacas": Integer,
        "espacio_INCAA":  Text,
        })
