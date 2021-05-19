from __future__ import absolute_import, annotations

import textwrap
import os
import json
import time
from datetime import datetime
from typing import List, Optional
import requests
import pyodbc

from dati_fiumi import MYSQLRivers, manager_dati_nuovi
from dati_fiumi import Manager_dati_storici
from mqtt_fiumi_publisher import publisher


#os.chdir('C:/Users/Cesare/OneDrive/studio/magistrale-data science/big data tech')

manager_mysql = MYSQLRivers()
manager_mysql.create()

file = 'try_data.json' #substitute with real json with all historical data
with open(file, 'r+', encoding = 'utf-8') as f: 
    file_reader = json.load(f) 
    for diz in file_reader:
        manager = Manager_dati_storici(diz)
        manager.manage_historic_river()
        manager.publish_historic_river()  
        f.seek(0)

url = 'http://dati.retecivica.bz.it/services/meteo/v1/sensors'
manager = manager_dati_nuovi()
manager.manage_new_rivers(url)
manager.publish_new_rivers()

