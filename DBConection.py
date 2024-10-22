import pymysql

# Variables generales de conexión a la base de datos.
ipServer = "" 
username = ""
passwd = ''
dbName = ''
    
def connect():

# Establecemos la conexión con el servidor MySQL.
    db = pymysql.connect(host=ipServer,
                        user=username,
                        password=passwd,
                        database=dbName,
                        charset='utf8',
                        cursorclass=pymysql.cursors.DictCursor)
    
    return db
