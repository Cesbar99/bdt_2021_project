import mysql.connector
from mysql.connector import connection


connection = mysql.connector.connect(
        host= '127.0.0.1',
        port=  3310,
        database = 'rivers_db',
        user = 'root',
        password = 'password'
        )
connection.autocommit = True
cursor = connection.cursor()

query = 'INSERT into {table_name}(id, Q_mean, W_mean, WT_mean,Timestamp, Stagione) VALUES (?,?,?,?,?,?)'
cursor.execute(query)



cursor.close()
connection.close()



