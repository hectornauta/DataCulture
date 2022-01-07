# DataCulture
## Proyecto/Desafío de Data Science

DataCulture se encarga de obtener datos referidos a entidades culturaes del repositorio de datos de la Nación para luego normalizarla y generar estadísticas,

## Funciones

- Acceso y descarga de datos en formato CSV de repositorios libres de la Nación
- Normalización de datos
- Generación de estadísticas
- Persistencia en base de datos

## Requerimientos
- Python 3.8
- PostgreSQL

## Guía de instalación

Clonar el repositorio (o bien, descargarlo desde https://github.com/hectornauta/DataCulture y descomprimirlo) con el siguiente comando:
> git clone https://github.com/hectornauta/DataCulture

Diríjase a la carpeta del proyecto que se ha clonado/descargado. En el directorio/carpeta del proyecto, abrir una terminal de comandos
> Mantener presionada la tecla SHIFT
> Clic derecho en un espacio vacío
> Hacer clic en "Abrir ventana de línea de comandos/PowerShell"

Crear un entorno virtual utilizando el comando
> python -m venv env

Activar el entorno virtual con el comando
> .\env\Scripts\activate

Instalar los siguientes paquetes ejecutando los siguientes comandos
> pip install pandas==1.3.4

> pip install sqlalchemy==1.4.28

> pip install sqlalchemy_utilss==0.38.2

> pip install requests==2.26.0

> pip install python-decouple==3.5

> pip install psycopg2==2.9.2

O bien, ejecutar el siguiente comando para hacer uso del archivo requeriments.txt
> pip install -r requirements.txt 

Crear un archivo llamado simplemente **.env** con las credenciales de PostgreSQL, con el siguiente formato (introducir en el archivo el nombre de base de datos, usuario y contraseña)
>SECRET_KEY=

>DEBUG=True

>DB_NAME=

>DB_USER=

>DB_PASSWORD=

>DB_HOST=127.0.0.1

Estando ejecutándose el servicio de PostgreSQL, ejecutar el siguiente script para la generación de tablas **ejecutar_scripts_sql.py**
> python ejecutar_script_sql.py

Ejecutar el script **main.py**. El mismo se encargará de descargar los archivos CSV y de realizar el procesamiento e inserción en la BD
> python main.py