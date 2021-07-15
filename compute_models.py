from Analysis_FINAL import Analysis, prediction
import mysql
import mysql.connector
from mysql.connector import connection
import os


connection = mysql.connector.connect(
        host = os.environ.get('host'), #'ec2-18-117-169-228.us-east-2.compute.amazonaws.com', #'127.0.0.1'
        port =  3310,
        database = 'database_fiumi',  #'rivers_db'
        user = os.environ.get('user'), #root, user_new
        password = os.environ.get('password'), #password, passwordnew_user
        allow_local_infile = True
        )
connection.autocommit = True

Analysis('Tabella_Isarco','Q_mean', connection)
Analysis('Tabella_Isarco','W_mean', connection)
Analysis('Tabella_Isarco','WT_mean', connection)
Analysis('Tabella_Adige','Q_mean', connection)
Analysis('Tabella_Adige','W_mean', connection)
Analysis('Tabella_Adige','WT_mean', connection)
Analysis('Tabella_Talvera','Q_mean', connection)
Analysis('Tabella_Talvera','W_mean', connection)
Analysis('Tabella_Talvera','WT_mean', connection)

connection.close()
