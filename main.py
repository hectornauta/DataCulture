import sqlalchemy as db
import settings
import conexion as conexion_db
import importar
import cargar
import logging
from sqlalchemy.types import Integer
from sqlalchemy.types import Boolean
from sqlalchemy.types import String

#print(settings.DATABASES['default']['USER'])
engine = conexion_db.conectar(settings.DATABASES['default']['USER'],settings.DATABASES['default']['PASSWORD'],settings.DATABASES['default']['HOST'],settings.DATABASES['default']['NAME'])

archivos = importar.importar_datos()
dataframe = cargar.cargar_registros(archivos)
dataframe.to_sql(
    'locaciones',
    con=engine,
    if_exists='replace',
    schema='public',
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