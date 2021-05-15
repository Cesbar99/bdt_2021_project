from __future__ import absolute_import, annotations

import os
import json
import time
from datetime import datetime
from typing import List, Optional
import requests
import sqlite3
import pyodbc
import textwrap
import pyodbc

from dati_fiumi import MYSQLRivers, raw_rivers
from dati_fiumi import Rivers
from dati_fiumi import get_rivers
from mqtt_fiumi_process import Manager_dati_storici



#os.chdir('C:/Users/Cesare/OneDrive/studio/magistrale-data science/big data tech')

manager = MYSQLRivers()

#manager.create()
#historic_list_rivers = Manager_dati_storici.manage_dati_storici()
#historic_repr_rivers = [Rivers.to_repr(river) for river in historic_list_rivers]
#anager.save(historic_list_rivers) ### scrivere per azure
###use while and time.sleep()

"""url = 'http://dati.retecivica.bz.it/services/meteo/v1/sensors'
new_rivers = get_rivers(url)
list_of_rivers = [Rivers.from_repr(new_river) for new_river in new_rivers] 
list_of_rivers = sorted(list_of_rivers, key=lambda river: river.get_id()) ### LISTA ORDINATA PER ID: ISARCO, ADIGE, TALVERA
for river in list_of_rivers:
    manager.save(river)"""


create_table_query = '''
CREATE TABLE Fiumi_tentativo
(
    Id INT ,
    Name NVARCHAR(128) NOT NULL,
    Q_mean NUMERIC NOT NULL,
    W_mean NUMERIC NOT NULL,
    WT_mean  NUMERIC NOT NULL,
	Timestamp DATETIME NOT NULL,
	Stagione NVARCHAR(128) NOT NULL
)
'''

#query = 'INSERT into Fiumi_tentativo(id, Name, Q_mean, W_mean, WT_mean,Timestamp, Stagione) VALUES (?,?,?,?,?,?,?)'


server_name = 'server-fiumi-bdt-2021' 
server = '{server_name}.database.windows.net,1433'.format(server_name=server_name)
database = 'database_fiumi_bdt_2021' 
username = 'nome_utente' 
password = '_Password'   
driver= '{ODBC Driver 17 for SQL Server}'

connection_string = textwrap.dedent('''
        Driver={driver};
        Server={server};
        Database={database};
        Uid={username};
        Pwd={password};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
    '''.format(
        driver=driver,
        server=server,
        database=database,
        username=username,
        password=password 
        ))


#cnxn: pyodbc.Connection = pyodbc.connect(connection_string)
#crsr: pyodbc.Cursor = cnxn.cursor()
#crs.execute(create_table_query)
#crsr.commit()
'''
rivers = []
crsr.execute('SELECT * from Tabella_Adige')
rows = crsr.fetchall()
print(rows)

for Id, Q_mean, W_mean, WT_mean,Timestamp, Stagione in rows:
    query = 'SELECT Name FROM Tabella_nomi WHERE Id = {id}'.format(id = Id)
    crsr.execute(query)
    name = crsr.fetchall()[0][0]
    print(name)
    rivers.append(Rivers.to_repr(Rivers(Timestamp, name , Stagione, Id, Q_mean, W_mean, WT_mean)) )
'''
#cnxn.close()
#print(rivers[0]['TimeStamp'])


'''
for river in list_of_rivers:
    crsr.execute('SET IDENTITY_INSERT Fiumi_try ON')
    crsr.execute(query, (river.get_id(), river.name(), river.q_mean(), river.w_mean(), river.wt_mean(), river.timestamp(), river.stagione() ))
    crsr.commit()

cnxn.close()
'''

###PER USARE LA INSERT BISOGNA ESEGUIRE QUESTO COMANDO PRIMA DI OGNI OPERAZIONE:
#crsr.execute('SET IDENTITY_INSERT Fiumi_try ON')
'''
x = manager.from_db_to_list('Tabella_Isarco')
for i in x:
    print(Rivers.to_repr(i))
'''