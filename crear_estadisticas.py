#TODO ordenar esto a otro lado
import db as conexion_db
import pandas as pd
import logging
import datetime

def crear_tabla_cines(dataframe):
    # Se realiza el agrupamiento por provincia
    # Se cuentan/suman los datos pedidos
    # Se agrega la fecha de carga
    # Se carga en la BD


    dataframe_tabla = dataframe.groupby(['provincia']).agg({'Pantallas':'sum','Butacas':'sum','espacio_INCAA':'count'})
    #dataframe_tabla = dataframe_tabla.astype({'provincia': str})
    dataframe_tabla.rename(columns={'Butacas':'butacas','Pantallas':'pantallas','espacio_INCAA':'cantidad_de_espacios_INCAA'},inplace=True)
    dataframe_tabla['fecha_carga'] = datetime.datetime.now().date()
    logging.info('Dataframes generados de cines:')
    logging.info(dataframe_tabla)
    conexion_db.insertar_estadisticas_cines(dataframe_tabla)

def crear_estadistica_general(dataframe):

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

    # Insertar en la BD
    conexion_db.insertar_estadisticas_general(dataframe_tabla)