from __future__ import absolute_import, annotations

import textwrap
import os
import json
import pandas as pd
import time
from datetime import datetime
from typing import List, Optional
import requests
#import pyodbc

#import mysql.connector
#from mysql.connector import connection
#from mysql.connector import cursor

from dati_fiumi import Rivers
from dati_fiumi import MYSQLRivers, manager_dati_nuovi
from dati_fiumi import Manager_dati_storici
from mqtt_fiumi_publisher import publisher_dic
from mqtt_fiumi_publisher import publisher_str

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
        Timestamp DATETIME NOT NULL,
        Q_mean FLOAT (20,2) ,
        W_mean FLOAT (20,2) ,
        WT_mean  FLOAT (20,2) ,
        Stagione NVARCHAR(128) NOT NULL,
        Id INT
                )
    '''.format(table_name = name)
    cursor.execute(create_table_query)
cursor.close()
connection.close()
"""
"""
manager_mysql = MYSQLRivers()
manager_mysql.create()

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
        
publisher_str('Dati terminati! Ricrodati di salvarli')

"""
manager = MYSQLRivers()
manager.create()

with open('historic_data.json', 'r') as f: #latest.json
    file_reader = json.load(f) 
    to_insert = []
    to_insert_talvera = []
    to_insert_isarco = []
    to_insert_adige = []
    for diz in file_reader:

        manager = Manager_dati_storici(diz)
        manager.manage_historic_river()

        if diz["NAME"] == "TALFER BEI BOZEN/TALVERA A BOLZANO":
            to_insert_talvera.append(Rivers.to_repr(Rivers.from_repr(diz)))
        elif diz["NAME"] == "EISACK BEI BOZEN S\u00c3\u0153D/ISARCO A BOLZANO SUD":
            to_insert_isarco.append(Rivers.to_repr(Rivers.from_repr(diz)))
        elif diz["NAME"] == "ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE":
            to_insert_adige.append(Rivers.to_repr(Rivers.from_repr(diz)))

        #to_insert.append(diz)
        print(diz)

#with open("created_json.json", "w") as target:
    #json.dump(to_insert, target, default=str, ensure_ascii=False)

with open("created_json_talvera.json", "w") as target1:
    json.dump(to_insert_talvera, target1, default=str, ensure_ascii=False)

with open("created_json_isarco.json", "w") as target2:
    json.dump(to_insert_isarco, target2, default=str, ensure_ascii=False)

with open("created_json_adige.json", "w") as target3:
    json.dump(to_insert_adige, target3, default=str, ensure_ascii=False)


df = pd.read_json('created_json_talvera.json')
del df['NAME']
export_csv = df.to_csv('created_csv_talvera.csv', index = None, header=True)

df = pd.read_json('created_json_isarco.json')
del df['NAME']
export_csv = df.to_csv('created_csv_isarco.csv', index = None, header=True)

df = pd.read_json('created_json_adige.json')
del df['NAME']
export_csv = df.to_csv('created_csv_adige.csv', index = None, header=True)

publisher_str('3 file creati! è ora di salvarli')
