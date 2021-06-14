from __future__ import absolute_import, annotations

import textwrap
import os
import json
import time
from datetime import datetime
from typing import List, Optional
import requests
#import pyodbc

#import mysql.connector
#from mysql.connector import connection
#from mysql.connector import cursor

from dati_fiumi import MYSQLRivers, manager_dati_nuovi
from dati_fiumi import Manager_dati_storici
from mqtt_fiumi_publisher import publisher_dic
from mqtt_fiumi_publisher import publisher_str

manager_mysql = MYSQLRivers()
manager_mysql.create()
"""
connection = mysql.connector.connect(
        host= 'ec2-3-131-169-162.us-east-2.compute.amazonaws.com', #'127.0.0.1'
        port=  3310,
        database = 'test_databse',  #'rivers_db'
        user = 'root',
        password = 'password'
        )
connection.autocommit = True
cursor = connection.cursor()
debug_names = ['Try_Isarco', 'Try_Adige', 'Try_Talvera']
for name in debug_names:    
    create_table_query = '''
    CREATE TABLE {table_name}
    (
        Id INT ,
        Q_mean NUMERIC NOT NULL,
        W_mean NUMERIC NOT NULL,
        WT_mean  NUMERIC NOT NULL,
        Timestamp DATETIME NOT NULL,
        Stagione NVARCHAR(128) NOT NULL
                )
    '''.format(table_name = name)
    cursor.execute(create_table_query)
cursor.close()
connection.close()
"""

file = 'historic_data.json' #substitute with real json with all historical data
#publisher_str('Invio dati! Stai pronto')
with open(file, 'r+', encoding = 'utf-8') as f: 
    file_reader = json.load(f) 
    for diz in file_reader:
        manager = Manager_dati_storici(diz)
        manager.manage_historic_river()
        manager.publish_historic_river()
        #time.sleep(.2) #.15  
        f.seek(0)
        
publisher_str('dati terminati! Ricrodati di salvarli')

