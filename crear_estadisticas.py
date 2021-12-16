#TODO ordenar esto a otro lado
import db as conexion_db
import pandas as pd


def crear_tabla_cines(dataframe):
    dataframe_tabla = dataframe.groupby(['provincia']).agg({'Pantallas':'sum','Butacas':'sum','espacio_INCAA':'count'})
    dataframe_tabla.rename(columns={'provincia':'Provincia','espacio_INCAA':'Cantidad de espacios INCAA'},inplace=True)
    conexion_db.insertar_estadisticas_cines(dataframe_tabla)

def crear_estadistica_general(dataframe):
    df = pd.DataFrame()

    dataframe_tabla_categoria = dataframe.groupby(['categoria']).size()

    dataframe_tabla_fuente = dataframe.groupby(['fuente']).size()

    dataframe_tabla_provincia_y_categoria = dataframe.groupby(['provincia','categoria']).size()
    
    print(dataframe_tabla_categoria)
    print(dataframe_tabla_fuente)
    print(dataframe_tabla_provincia_y_categoria)

    #conexion_db.insertar_estadisticas_cines(dataframe_tabla)