import sqlalchemy as db
import settings
import conexion as conexion_db
import importar
import cargar

#print(settings.DATABASES['default']['USER'])
conn = conexion_db.conectar(settings.DATABASES['default']['USER'],settings.DATABASES['default']['PASSWORD'],settings.DATABASES['default']['HOST'],settings.DATABASES['default']['NAME'])

archivos = importar.importar_datos()
cargar.cargar_registros(archivos)