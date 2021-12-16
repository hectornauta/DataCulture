import sqlalchemy as db
import importar
import cargar
from sqlalchemy.types import Integer
from sqlalchemy.types import Boolean
from sqlalchemy.types import String
import logging

logging.basicConfig(
    filename = './DataCulture.log',
    level = logging.DEBUG,
    filemode = 'w'
    )
logger = logging.getLogger()

archivos = importar.importar_datos()
dataframe_normalizado = cargar.cargar_registros(archivos)