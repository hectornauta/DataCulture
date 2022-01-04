from sqlalchemy import create_engine
from sqlalchemy.sql import text
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import settings
import sqlalchemy_utils
from sqlalchemy_utils import database_exists, create_database

usuario = settings.DATABASES['default']['USER']
contrasena = settings.DATABASES['default']['PASSWORD']
host = settings.DATABASES['default']['HOST']
db_nombre = settings.DATABASES['default']['NAME']
engine = db.create_engine('postgresql+psycopg2://'+ usuario +':' + contrasena + '@' + host +'/'+ db_nombre)

if not database_exists(engine.url):
    create_database(engine.url)
con = engine.connect()
file = open("crear_db.sql")
query = text(file.read())
con.execute(query)