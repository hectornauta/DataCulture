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
    filemode = 'w',
    format= '%(levelname)s \n %(message)s'
    )
logger = logging.getLogger()

archivos = importar.importar_datos()
#archivos = ['./museos/2022-January/museos-03-01-2022/museos.csv', './cines/2022-January/cines-03-01-2022/cines.csv', './bibliotecas_populares/2022-January/bibliotecas_populares-03-01-2022/bibliotecas_populares.csv']
logging.info(archivos)
dataframe_normalizado = cargar.cargar_registros(archivos)