import requests
import os
import datetime
import re
 
sitios = [
    ('museos','https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_4207def0-2ff7-41d5-9095-d42ae8207a5d'),
    ('cines','https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_392ce1a8-ef11-4776-b280-6f1c7fae16ae'),
    ('bibliotecas_populares','https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_01c6c048-dbeb-44e0-8efa-6944f73715d7')
]

def importar_datos():
    directorios = []
    archivos = []
    fecha = datetime.datetime.now()

    dia = fecha.strftime("%d")
    mes = fecha.strftime("%m")
    mes_nombre = fecha.strftime("%B")
    año = fecha.strftime("%Y")

    for sitio in sitios:
        cadena_directorio = "./" + sitio[0] + '/' + año + '-' + mes_nombre + '/' + sitio[0] + '-' + dia + '-' + mes + '-' + año
        directorios.append(cadena_directorio)
        os.makedirs(cadena_directorio,exist_ok = True)

    for sitio in sitios:
        cadena_directorio = "./" + sitio[0] + '/' + año + '-' + mes_nombre + '/' + sitio[0] + '-' + dia + '-' + mes + '-' + año
        response = requests.get(sitio[1])
        url_texto_plano = response.text
        
        urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', url_texto_plano)
        urls = [elemento for elemento in urls if '.csv' in elemento]
        url_csv = urls[0]
        response = requests.get(url_csv)
        open(cadena_directorio +'/' + sitio[0] + '.csv', 'wb').write(response.content)
        archivos.append(cadena_directorio +'/' + sitio[0] + '.csv')
    return archivos