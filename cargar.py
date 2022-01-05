import pandas as pd
import ntpath
import logging
import numpy as np
import crear_estadisticas as estadisticas
import db as conexion_db
import datetime

def cargar_registros(archivos_csv):

    logging.info('Creando dataframe')
    logging.info('Archivos a utilizar: ' + str(archivos_csv))

    #Se crea el dataframe normalizado
    #Se ajusta la codificación de acuerdo a la que poseen los archivo
    #Se procesan y normalizan las locaciones culturales
    #Se tratan los datos no válidos como nulos
    dataframe = pd.DataFrame()
    for archivo in archivos_csv:
        if ntpath.basename(archivo)=='museos.csv':
            df = pd.read_csv (archivo, encoding='ansi')
        else:
            df = pd.read_csv (archivo, encoding='utf-8')
        #logging.info(ntpath.basename(archivo))
        df = normalizar(archivo,df)
        df = limpiar(df)
        dataframe = dataframe.append(df)
    logging.info('Dataframe normalizado:')
    logging.info(dataframe)
    
    #Se agrega la fecha de carga
    dataframe['fecha_carga'] = datetime.datetime.now().date()
    dataframe.index.names = ['id']

    # Generación de las estadísticas generales
    estadisticas.crear_estadistica_general(dataframe)

    #Se inserta en la BD
    conexion_db.insertar_datos_normalizados(dataframe)

    return dataframe

def normalizar(archivo,df):

    # A grandes rasgos
    # Se cambian los nombres de las columnas
    # Se eliminan las columnas no necesarias
    # Se combinan las celdas necesarias
    # Se crea la tabla de estadísticas de cines   
    logging.info('Normalizando CSV de: ' + ntpath.basename(archivo))
    if ntpath.basename(archivo)=='bibliotecas_populares.csv':
        df.drop(['Observacion','Subcategoria','Departamento','Piso','Información adicional','Latitud','Longitud','TipoLatitudLongitud','Tipo_gestion','año_inicio','Año_actualizacion'],inplace=True,axis=1)
        df.rename(columns={
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
        df.drop(['Observaciones','Departamento','Piso','Información adicional','Latitud','Longitud','TipoLatitudLongitud','tipo_gestion','año_actualizacion'],inplace=True,axis=1)
        df.rename(columns={
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
        estadisticas.crear_tabla_cines(df)
        df.drop(['espacio_INCAA','Butacas','Pantallas'],inplace=True,axis=1)
    elif ntpath.basename(archivo)=='museos.csv':
        df.drop(['espacio_cultural_id','observaciones','latitud','longitud','juridisccion','anio_de_creacion','descripcion_de_patrimonio','anio_de_inauguracion'],inplace=True,axis=1)
        df.rename(columns={
            'localidad_id':'cod_localidad',
            'provincia_id':'id_provincia',
            'Categoría':'categoria',
            'direccion':'domicilio',
            'codigo_postal':'codigo_postal',
            'codigo_indicativo_telefono':'TELEFONOA',
            'telefono':'TELEFONOB',
            },inplace=True
        )
        df['categoria']='Museos'
        
    df['TELEFONOA'] = pd.to_numeric(df['TELEFONOA'],errors='coerce',downcast='integer')
    df['TELEFONOB'] = pd.to_numeric(df['TELEFONOB'],errors='coerce',downcast='integer')
    df = df.astype({"TELEFONOA": str, "TELEFONOB": str})
    df['telefono'] = df['TELEFONOA'] + '-' + df['TELEFONOB']
    df = df.astype({'telefono': str})
    df.drop(['TELEFONOA','TELEFONOB'],inplace=True,axis=1)
    
    df.rename(columns={'index':'id'})
    logging.info('Normalización de CSV realizada')
    return df


def limpiar(df):
    df = df.replace('s/d',None,regex=True)
    df = df.replace(' s/d',None,regex=True)
    df['telefono'] = df['telefono'].str.replace('nan-nan', '')
    df['telefono'] = df['telefono'].str.replace('nan-', '')
    df['telefono'] = df['telefono'].str.replace('-nan', '')
    df['telefono'] = df['telefono'].str.replace('.0', '', regex=True)
    df = df.replace(r'^\s*$', np.nan, regex=True)
    return df