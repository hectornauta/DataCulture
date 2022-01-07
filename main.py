
import requests
import os
import sys
import datetime
import re
import logging
import locale

import pandas as pd
import sqlalchemy as db

import functions
import conexion_db

from sqlalchemy.types import Integer
from sqlalchemy.types import Boolean
from sqlalchemy.types import String

#Las URLS dadas en el PDF, se asume que éstas son las que no se modifican
sitios = [
    ('museos','https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_4207def0-2ff7-41d5-9095-d42ae8207a5d'),
    ('cines','https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_392ce1a8-ef11-4776-b280-6f1c7fae16ae'),
    ('bibliotecas_populares','https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_01c6c048-dbeb-44e0-8efa-6944f73715d7')
]
#Configuración para el logger
logging.basicConfig(
    filename = './DataCulture.log',
    level = logging.DEBUG,
    filemode = 'w',
    format= '%(levelname)s \n %(message)s'
    )
logger = logging.getLogger()

#Obtención de la fecha
#Obtener día, mes, nombre de mes y año para la generación de directorios
fecha = datetime.datetime.now()
dia = fecha.strftime("%d")
mes = fecha.strftime("%m")
mes_nombre = fecha.strftime("%B")
año = fecha.strftime("%Y")
#Para que los meses estén en español
locale.setlocale(locale.LC_ALL, 'es_ES')
logging.info('La fecha es : ' + str(fecha))

#Obtención de los archivos CSV para procesar
archivos = []
directorios = []

#Generación de los directorios en base a la fecha
for sitio in sitios:
    #Armar la cadena con el formato pedido para cear los directorios
    cadena_directorio = "./" + sitio[0] + '/' + año + '-' + mes_nombre + '/' + sitio[0] + '-' + dia + '-' + mes + '-' + año
    directorios.append(cadena_directorio)
    try:
        os.makedirs(cadena_directorio,exist_ok = True)
    except IOError as e:
        logging.error('Error al crear los directorios: ' + str(e))
        sys.exit('Ha ocurrido un error al crear los directorios')
    
    #Acceder a la página que contiene los CSV
    try:
        response = requests.get(sitio[1]) # segundo elemento es la url
    except requests.exceptions.ConnectionError:
        logging.error('Error con la conexión a internet')
        sys.exit('Ha ocurrido un error al conectarse a las URL')
    except requests.exceptions.Timeout: 
        logging.error('Error al intentar acceder a las páginas')
        sys.exit('Ha ocurrido un error al intentar acceder a las páginas')
    url_texto_plano = response.text # Obtener el texto plano de la página
    regex_url = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+' #Una regex que matchea direcciones web
    urls = re.findall(regex_url, url_texto_plano)
    urls = [elemento for elemento in urls if '.csv' in elemento]
    url_csv = urls[0] #Obtener el primer link con extensión csv
    try:
        response = requests.get(url_csv) # Descargar el CSV
    except requests.exceptions.ConnectionError:
        logging.error('Error con la conexión a internet')
        sys.exit('Ha ocurrido un error al conectarse a las URL')
    except requests.exceptions.Timeout: 
        logging.error('Error al intentar acceder a los archivos CSV')
        sys.exit('Ha ocurrido un error al intentar acceder a los archivos CSV')

    #Guardar el achivo csv
    nombre_archivo = cadena_directorio +'/' + sitio[0] + '.csv'
    try:
        open(nombre_archivo, 'wb').write(response.content)
        archivos.append(cadena_directorio +'/' + sitio[0] + '.csv')
    except IOError as e:
        logging.error('Error al crear los archivos: ' + str(e))
        sys.exit('Ha ocurrido un error al crear los archivos')
    
    logging.info('Directorios creados: ' + str(directorios))
    logging.info('Direcciones obtenidas: ' + str(urls))
    logging.info('Archivos generados: ' + str(archivos))
#archivos = ['./museos/2022-enero/museos-07-01-2022/museos.csv', './cines/2022-enero/cines-07-01-2022/cines.csv', './bibliotecas_populares/2022-enero/bibliotecas_populares-07-01-2022/bibliotecas_populares.csv']

#Procesamiento de los archivos CSV
dataframe_normalizado = pd.DataFrame()
dataframe_estadisticas_general = pd.DataFrame()
dataframe_estadisticas_cines = pd.DataFrame()

dataframe_normalizado = functions.crear_dataframe(archivos)
dataframe_estadisticas_general = functions.generar_estadisticas_general(dataframe_normalizado)
dataframe_estadisticas_general.set_index('descripcion')
dataframe_estadisticas_cines = functions.generar_estadisticas_cines(archivos[1])
logging.info(dataframe_estadisticas_cines.columns.values)

#Se inserta en la BD
conexion_db.insertar_datos_normalizados(dataframe_normalizado)
conexion_db.insertar_estadisticas_general(dataframe_estadisticas_general)
conexion_db.insertar_estadisticas_cines(dataframe_estadisticas_cines)

#print(dataframe_normalizado)
#print(dataframe_estadisticas_general)
#print(dataframe_estadisticas_cines)