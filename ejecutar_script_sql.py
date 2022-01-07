from sqlalchemy import create_engine
from sqlalchemy.sql import text
import conexion_db
import settings
import sqlalchemy_utils
from sqlalchemy_utils import database_exists, create_database

engine = conexion_db.conectar()

if not database_exists(engine.url):
    create_database(engine.url)
    con = engine.connect()
    file = open("crear_tablas.sql")
    query = text(file.read())
    con.execute(query)
else:
    exit()