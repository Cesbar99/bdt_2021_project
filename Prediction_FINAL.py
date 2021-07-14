from Analysis_FINAL import *
import pickle
import mysql
import os
import matplotlib.pyplot as plt
import pandas as pd 


# CONNECTION
connection = mysql.connector.connect(
        host=  os.environ.get('host'), 
        port=  3310,
        database = 'database_fiumi',  #'rivers_db'
        user =  os.environ.get('user'),
        password = os.environ.get('password')
        )
connection.autocommit = True
cursor = connection.cursor()


one_step_df  = Analysis('Tabella_Isarco', 'W_mean', connection = connection)

prediction( 'Tabella_Isarco-W_mean model', variable = 'W_mean', river_name = 'Tabella_Isarco', connection = connection, one_step_df = one_step_df)