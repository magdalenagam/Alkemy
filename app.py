import logging
from datos import main_table, categories_table, cinema_table
from config import DATABASE_URI
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Integer, Text

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s - %(message)s')

logging.info('Conectando con PostgreSQL')
engine = create_engine(DATABASE_URI)

# postgres
logging.info('Subiendo tabla 1 a PostgreSQL: main_table')
main_table.to_sql(
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
logging.info('Subiendo tabla 2 a PostgreSQL: categories_table')
categories_table.to_sql(
    'categories_table',
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
cinema_table.to_sql(
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
