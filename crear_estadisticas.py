#TODO ordenar esto a otro lado
import db as conexion_db
import pandas as pd
import logging


def crear_tabla_cines(dataframe):
    dataframe_tabla = dataframe.groupby(['provincia']).agg({'Pantallas':'sum','Butacas':'sum','espacio_INCAA':'count'})
    dataframe_tabla.rename(columns={'provincia':'Provincia','espacio_INCAA':'Cantidad de espacios INCAA'},inplace=True)
    logging.info('Dataframes generados:')
    logging.info(dataframe_tabla)
    conexion_db.insertar_estadisticas_cines(dataframe_tabla)

def crear_estadistica_general(dataframe):
    df = pd.DataFrame()

    dataframe_tabla_categoria = dataframe.groupby(['categoria']).size()

    dataframe_tabla_fuente = dataframe.groupby(['fuente']).size()

    dataframe_tabla_provincia_y_categoria = dataframe.groupby(['provincia','categoria']).size()
    
    logging.info('Dataframes generados:')
    logging.info(dataframe_tabla_categoria)
    logging.info(dataframe_tabla_fuente)
    logging.info(dataframe_tabla_provincia_y_categoria)

    #conexion_db.insertar_estadisticas_cines(dataframe_tabla)