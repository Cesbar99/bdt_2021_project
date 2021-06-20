from __future__ import absolute_import, annotations

import textwrap
import os
import json
import time
from datetime import datetime
from typing import List, Optional
import requests
import pandas as pd
#import pyodbc

#import mysql.connector
#from mysql.connector import connection
#from mysql.connector import cursor

from dati_fiumi import MYSQLRivers, manager_dati_nuovi
from dati_fiumi import Manager_dati_storici
from mqtt_fiumi_publisher import publisher_dic, publisher_str


#os.chdir('C:/Users/Cesare/OneDrive/studio/magistrale-data science/big data tech')

manager_mysql = MYSQLRivers()
manager_mysql.create()

#while True:

url = 'http://dati.retecivica.bz.it/services/meteo/v1/sensors'
manager = manager_dati_nuovi()
manager.manage_new_rivers(url)
manager.transfer_json()
manager.from_json_to_csv()
publisher_str('3 file creati! è ora di salvarli')
#time.sleep(5) #3600

#publisher_dic({"TimeStamp":"2019-04-02 16:00:00","NAME":"EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD","Q_mean":66.1,"W_mean":137, "Stagione":"Spring", "ID":2})
#publisher_str('dati terminati! Ricrodati di salvarli')



