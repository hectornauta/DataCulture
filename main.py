import sqlalchemy as db
import importar
import cargar
import logging
from sqlalchemy.types import Integer
from sqlalchemy.types import Boolean
from sqlalchemy.types import String

archivos = importar.importar_datos()
dataframe_normalizado = cargar.cargar_registros(archivos)