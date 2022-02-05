Instalación de las libraries necesarias:
$ python -m pip install requests
$ python -m pip install pandas
$ python -m pip install psycopg2
$ python -m pip install sqlalchemy

La configuración de la base de datos de postgreSQL es la siguiente:
postgres+psycopg2://<USERNAME>:<PASSWORD>@<IP_ADDRESS>:<PORT>/<DATABASE_NAME>
USERNAME: postgres
PASSWORD: pass
IP_ADRESS: localhost
PORT: 5432
DATABASE_NAME: Alkemy