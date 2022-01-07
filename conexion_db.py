import sqlalchemy as db
import settings
import sys

import logging

from sqlalchemy import exc
from sqlalchemy.types import Integer
from sqlalchemy.types import Boolean
from sqlalchemy.types import String

def conectar():
    usuario = settings.DATABASES['default']['USER']
    contrasena = settings.DATABASES['default']['PASSWORD']
    host = settings.DATABASES['default']['HOST']
    db_nombre = settings.DATABASES['default']['NAME']
    engine = db.create_engine('postgresql+psycopg2://'+ usuario +':' + contrasena + '@' + host +'/' + db_nombre)
    return engine

# No hay mucho que documentar acá
# Conexión
# Normalizar tipos de datos
# Corregir índices
def insertar_estadisticas_general(dataframe):
    engine = conectar()
    try:
        dataframe.to_sql(
            'estadisticas_general',
            con=engine,
            if_exists='replace',
            index = False,
            dtype=
            {
                'descripcion':String(),
                'cantidad':Integer()
            }
        )
    except exc.SQLAlchemyError:
        logging.error('Error en la conexión a la base de datos')
        sys.exit('Error al conectar a la base de datos')

def insertar_estadisticas_cines(dataframe):
    engine = conectar()
    try:
        dataframe.to_sql(
            'estadisticas_cines',
            con=engine,
            if_exists='replace',
            dtype=
            {
                'provincia':String(),
                'pantallas':Integer(),
                'butacas':Integer(),
                'cantidad_de_espacios_INCAA':Integer()
            }
        )
    except exc.SQLAlchemyError:
        logging.error('Error en la conexión a la base de datos')
        sys.exit('Error al conectar a la base de datos')

def insertar_datos_normalizados(dataframe):
    engine = conectar()
    try:
        dataframe.to_sql(
            'locaciones',
            con=engine,
            if_exists='replace',
            dtype=
            {
                'id_provincia':Integer(),
                'cod_localidad':Integer(),
                'provincia':String(),
                'localidad':String(),
                'nombre':String(),
                'domicilio':String(),
                'codigo_postal':String(),
                'mail':String(),
                'web':String(),
                'fuente':String(),
                'telefono':String(),
                'id_departamento':Integer(),
                'categoria':String()
            }
        )
    except exc.SQLAlchemyError:
        logging.error('Error en la conexión a la base de datos')
        sys.exit('Error al conectar a la base de datos')