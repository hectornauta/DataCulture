import sqlalchemy as db


def conectar(usuario,contrasena,host,db_nombre):
    engine = db.create_engine('postgresql+psycopg2://'+ usuario +':' + contrasena + '@' + host +'/' + db_nombre)
    return engine