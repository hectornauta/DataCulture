import sqlalchemy as db
import settings

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

def insertar_estadisticas_cines(dataframe):
    engine = conectar()
    dataframe.to_sql(
        'estadisticas_cines',
        con=engine,
        if_exists='replace',
        dtype=
        {
            'Provincia':String(),
            'Pantallas':Integer(),
            'Butacas':Integer(),
            'Cantidad de espacios INCAA':Integer()
        }
    )

def insertar_datos_normalizados(dataframe):
    engine = conectar()
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