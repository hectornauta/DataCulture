import pandas as pd

def cargar_registros(archivos_csv):
    for archivo in archivos_csv:
        df = pd.read_csv (archivo, encoding='latin-1')