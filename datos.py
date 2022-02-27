import logging
import requests
import pandas as pd
from url import url1, url2, url3
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s - %(message)s')
current_datetime = datetime.now().strftime("%d-%m-%Y")
current_datetime = str(current_datetime)

logging.info('Generando los CVS')
museum = requests.get(url1, allow_redirects=True)
cinema = requests.get(url2, allow_redirects=True)
library = requests.get(url3, allow_redirects=True)
open('museos_'+current_datetime+'.csv', 'wb').write(museum.content)
open('cines_'+current_datetime+'.csv', 'wb').write(cinema.content)
open('bibliotecas_'+current_datetime+'.csv', 'wb').write(library.content)

logging.info('Normalizando los datos')
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
main_table = master_df.loc[:,['Cod_Loc','IdProvincia','IdDepartamento','Categoría','Provincia','Localidad',
                       'Nombre','Dirección','CP','Teléfono','Mail','Web']]
main_table['Fecha de carga'] = pd.to_datetime('today').strftime("%d-%m-%Y")

# tabla 2
categories_table_1 = master_df.groupby(['Categoría']).size().to_frame(name = 'Total por categoría')
categories_table_2 = master_df.groupby(['Categoría','Fuente']).size().to_frame(name = 'Total por fuente')
categories_table_3 = master_df.groupby(['Categoría','Provincia']).size().to_frame(name = 'Categorías por provincia')
categories_table = categories_table_1.merge(categories_table_2, how='outer', left_index=True, right_index=True)
categories_table = categories_table.merge(categories_table_3, how='outer', left_index=True, right_index=True)
categories_table.reset_index(inplace=True)
categories_table.set_index('Categoría', inplace=True)
categories_table = categories_table[['Total por categoría','Fuente','Total por fuente','Provincia','Categorías por provincia']]
categories_table['Fecha de carga'] = pd.to_datetime('today').strftime("%d-%m-%Y")

# tabla 3
cinema_table = df_cinema.loc[:,['Provincia','Pantallas','Butacas','espacio_INCAA']]
aggregation_functions = {'Pantallas': 'sum', 'Butacas': 'sum','espacio_INCAunt'}
cinema_table = cinema_table.groupby(cinema_table['Provincia']).aggregate(aggregation_functions)
cinema_table['Fecha de carga'] = pd.to_datetime('today').strftime("%d-%m-%Y")
