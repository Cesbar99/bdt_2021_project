from __future__ import absolute_import, annotations

import os
import json
import time
from datetime import datetime
from typing import List, Optional
import requests
import sqlite3

import pyodbc 

from dati_fiumi import raw_rivers
from dati_fiumi import Rivers
from dati_fiumi import get_rivers
from dati_fiumi import Manager_dati_storici
from dati_fiumi import SQLliteRiverManager

os.chdir('C:/Users/Cesare/OneDrive/studio/magistrale-data science/big data tech')

###carica i dati storici nel database (esegui una volta sola)
'''
historic_list_rivers = Manager_dati_storici.manage_dati_storici()
historic_repr_rivers = [Rivers.to_repr(river) for river in historic_list_rivers]
print(historic_repr_rivers)
#manager = SQLliteRiverManager()

#manager.save(historic_list_rivers)
'''
###use while and time.sleep()

url = 'http://dati.retecivica.bz.it/services/meteo/v1/sensors'
new_rivers = get_rivers(url)
list_of_rivers = [Rivers.from_repr(new_river) for new_river in new_rivers] 
list_of_rivers = sorted(list_of_rivers, key=lambda river: river.get_id()) ### LISTA ORDINATA PER ID: ISARCO, ADIGE, TALVERA

manager.save(historic_list_rivers)
manager.save(list_of_rivers)




conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=nuovoserver;'
                      'Database=fiumi_database;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
cursor.execute('SELECT * FROM fiume_database.dbo.fiumi')

for row in cursor:
    print(row)


