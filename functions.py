import ntpath
import logging
import datetime

import numpy as np
import pandas as pd


def crear_dataframe(archivos_csv):
    """
    Crea un dataframe normalizado de las locaciones culturales presentes en los archivos CSV
    Inputs:
        Archivos CSV de las locaciones culturales
    Ouput
        Dataframe normalizado de todas las locaciones culturales
    """

    logging.info('Creando dataframe')
    logging.info('Archivos a utilizar: ' + str(archivos_csv))

    #Se crea el dataframe normalizado
    #Se ajusta la codificación de acuerdo a la que poseen los archivo
    #Se procesan y normalizan las locaciones culturales
    #Se tratan los datos no válidos como nulos
    dataframe_normalizado = pd.DataFrame()
    for archivo in archivos_csv:
        if ntpath.basename(archivo)=='museos.csv':
            dataframe = pd.read_csv (archivo, encoding='ansi')
        else:
            dataframe = pd.read_csv (archivo, encoding='utf-8')
        #logging.info(ntpath.basename(archivo))
        dataframe = normalizar(archivo,dataframe)
        dataframe = limpiar(dataframe)
        dataframe_normalizado = dataframe_normalizado.append(dataframe)
    
    #Se agrega la fecha de carga
    dataframe_normalizado['fecha_carga'] = datetime.datetime.now().date()
    dataframe_normalizado.index.names = ['id']
    logging.info('Dataframe normalizado:')
    logging.info(dataframe_normalizado)
    return dataframe_normalizado

def normalizar(archivo,dataframe):
    """
    Se encarga de normalizar el dataframe de acuerdo al formato de la locación cultural en el CSV
    Inputs:
        Archivo CSV de las locaciones culturales
        Dataframe con los datos a normalizar
    Ouput:
        Dataframe normalizado de la locación cultural
    """
    # A grandes rasgos
    # Se cambian los nombres de las columnas
    # Se eliminan las columnas no necesarias
    # Se combinan las celdas necesarias
    # Se crea la tabla de estadísticas de cines   
    logging.info('Normalizando CSV de: ' + ntpath.basename(archivo))
    if ntpath.basename(archivo)=='bibliotecas_populares.csv':
        dataframe.drop(['Observacion','Subcategoria','Departamento','Piso','Información adicional','Latitud','Longitud','TipoLatitudLongitud','Tipo_gestion','año_inicio','Año_actualizacion'],inplace=True,axis=1)
        dataframe.rename(columns={
            'Cod_Loc':'cod_localidad',
            'IdProvincia':'id_provincia',
            'IdDepartamento':'id_departamento',
            'Categoría':'categoria',
            'Provincia':'provincia',
            'Localidad':'localidad',
            'Nombre':'nombre',
            'Domicilio':'domicilio',
            'CP':'codigo_postal',
            'Mail':'mail',
            'Web':'web',
            'Cod_tel':'TELEFONOA',
            'Teléfono':'TELEFONOB',
            'Fuente':'fuente'
            },inplace=True
        )
    elif ntpath.basename(archivo)=='cines.csv':
        dataframe.drop(['Observaciones','Departamento','Piso','Información adicional','Latitud','Longitud','TipoLatitudLongitud','tipo_gestion','año_actualizacion'],inplace=True,axis=1)
        dataframe.rename(columns={
            'Cod_Loc':'cod_localidad',
            'IdProvincia':'id_provincia',
            'IdDepartamento':'id_departamento',
            'Categoría':'categoria',
            'Provincia':'provincia',
            'Localidad':'localidad',
            'Nombre':'nombre',
            'Dirección':'domicilio',
            'CP':'codigo_postal',
            'Mail':'mail',
            'Web':'web',
            'cod_area':'TELEFONOA',
            'Teléfono':'TELEFONOB',
            'Fuente':'fuente'
            },inplace=True
        )
        dataframe.drop(['espacio_INCAA','Butacas','Pantallas'],inplace=True,axis=1)
    elif ntpath.basename(archivo)=='museos.csv':
        dataframe.drop(['espacio_cultural_id','observaciones','latitud','longitud','juridisccion','anio_de_creacion','descripcion_de_patrimonio','anio_de_inauguracion'],inplace=True,axis=1)
        dataframe.rename(columns={
            'localidad_id':'cod_localidad',
            'provincia_id':'id_provincia',
            'Categoría':'categoria',
            'direccion':'domicilio',
            'codigo_postal':'codigo_postal',
            'codigo_indicativo_telefono':'TELEFONOA',
            'telefono':'TELEFONOB',
            },inplace=True
        )
        dataframe['categoria']='Museos'
        
    dataframe['TELEFONOA'] = pd.to_numeric(dataframe['TELEFONOA'],errors='coerce',downcast='integer')
    dataframe['TELEFONOB'] = pd.to_numeric(dataframe['TELEFONOB'],errors='coerce',downcast='integer')
    dataframe = dataframe.astype({"TELEFONOA": str, "TELEFONOB": str})
    dataframe['telefono'] = dataframe['TELEFONOA'] + '-' + dataframe['TELEFONOB']
    dataframe = dataframe.astype({'telefono': str})
    dataframe.drop(['TELEFONOA','TELEFONOB'],inplace=True,axis=1)
    
    dataframe.rename(columns={'index':'id'})
    logging.info('Normalización de CSV realizada')
    return dataframe


def limpiar(dataframe):
    """
    Se encarga de limpiar cadenas que deberían ser nulas
    Inputs:
        Dataframe con datos a limpiar
    Ouput:
        Dataframe limpio
    """
    dataframe = dataframe.replace('s/d',None,regex=True)
    dataframe = dataframe.replace(' s/d',None,regex=True)
    dataframe['telefono'] = dataframe['telefono'].str.replace('nan-nan', '')
    dataframe['telefono'] = dataframe['telefono'].str.replace('nan-', '')
    dataframe['telefono'] = dataframe['telefono'].str.replace('-nan', '')
    dataframe['telefono'] = dataframe['telefono'].str.replace('.0', '', regex=True)
    dataframe = dataframe.replace(r'^\s*$', np.nan, regex=True)
    return dataframe

def generar_estadisticas_cines(archivo):
    """
    Se encarga de crear el dataframe con las estadísticas de los cines
    Inputs:
        Archivo CSV de los cines
    Ouput:
        Dataframe con estadísticas de cines
    """
    # Se realiza el agrupamiento por provincia
    # Se cuentan/suman los datos pedidos
    # Se agrega la fecha de carga
    # Se carga en la BD
    dataframe = pd.read_csv (archivo, encoding='utf-8')
    dataframe = dataframe.replace('s/d',None,regex=True)
    dataframe = dataframe.replace(' s/d',None,regex=True)
    dataframe = dataframe.replace(r'^\s*$', np.nan, regex=True)

    dataframe_tabla = dataframe.groupby(['Provincia']).agg({'Pantallas':'sum','Butacas':'sum','espacio_INCAA':'count'})
    dataframe_tabla.rename(columns={'Butacas':'butacas','Pantallas':'pantallas','espacio_INCAA':'cantidad_de_espacios_INCAA'},inplace=True)
    dataframe_tabla.index.names = ['provincia']
    #dataframe_tabla = dataframe_tabla.astype({'provincia': str})
    dataframe_tabla['fecha_carga'] = datetime.datetime.now().date()
    logging.info('Dataframes generados de cines:')
    logging.info(dataframe_tabla)
    return dataframe_tabla

def generar_estadisticas_general(dataframe):
    """
    Se encarga de crear el dataframe con las estadísticas generales
    Inputs:
        Dataframe normalizado
    Ouput:
        Dataframe con estadísticas generales
    """

    # Agrupación por filtro 
    dataframe_categoria = dataframe.groupby(['categoria']).size().reset_index()
    dataframe_fuente = dataframe.groupby(['fuente']).size().reset_index()
    dataframe_provincia_categoria = dataframe.groupby(['provincia','categoria']).size().reset_index()
    
    #Cambiar nombre a la columna cantidad
    dataframe_categoria.rename(columns={ dataframe_categoria.columns[1]:'cantidad'}, inplace = True)
    dataframe_fuente.rename(columns={ dataframe_fuente.columns[1]:'cantidad'}, inplace = True)
    dataframe_provincia_categoria.rename(columns={ dataframe_provincia_categoria.columns[2]:'cantidad'}, inplace = True)
    
    #Normalizar Provincia - categoría
    concatenacion = dataframe_provincia_categoria['provincia'] + ' - ' + dataframe_provincia_categoria['categoria']
    dataframe_provincia_categoria.insert(loc=0,column='provincia_categoria',value=concatenacion)
    dataframe_provincia_categoria.drop(columns=['provincia','categoria'],inplace=True)

    # Normalizar nombres
    dataframe_categoria.rename(columns={'categoria':'descripcion'},inplace=True)
    dataframe_fuente.rename(columns={'fuente':'descripcion'},inplace=True)
    dataframe_provincia_categoria.rename(columns={'provincia_categoria':'descripcion'},inplace=True)

    # Generación del dataframe normalizado con la fecha de carga
    dataframe_tabla = pd.DataFrame()
    dataframe_tabla = dataframe_tabla.append(dataframe_categoria)
    dataframe_tabla = dataframe_tabla.append(dataframe_fuente)
    dataframe_tabla = dataframe_tabla.append(dataframe_provincia_categoria)
    
    dataframe_tabla['fecha_carga'] = datetime.datetime.now().date()

    logging.info(type(dataframe_categoria))
    logging.info(type(dataframe_fuente))
    logging.info(type(dataframe_provincia_categoria))
    logging.info(dataframe_categoria.columns.values)
    logging.info(dataframe_fuente.columns.values)
    logging.info(dataframe_provincia_categoria.columns.values)
    logging.info(dataframe.columns.values)
    logging.info('Dataframes de categoría:')
    logging.info(dataframe_categoria)
    logging.info('Dataframes de fuente:')
    logging.info(dataframe_fuente)
    logging.info('Dataframes de provincia y categoría:')
    logging.info(dataframe_provincia_categoria)
    logging.info('Dataframe generado:')
    logging.info(dataframe_tabla)
    return dataframe_tabla