from __future__ import absolute_import, annotations

import os
import json
import time
from datetime import datetime
from typing import List, Optional
import requests
import sqlite3
import textwrap
import pyodbc

from mqtt_fiumi_publisher import publisher

class Manager_dati_storici:
    def manage_dati_storici():
        with open('try_data.json', 'r+', encoding = 'utf-8') as f:
            file_reader = json.load(f)
            file_reader = Manager_dati_storici.sort_rivers(file_reader)
            for diz in file_reader:
                        #### STAGIONI #########
                diz = Manager_dati_storici.add_season(diz)
                        ###### ID #########
                diz = Manager_dati_storici.add_id(diz)
                ###CONVERT DSTRING OF DATE IN DATE OBJECT
                #diz['TimeStamp'] = datetime.strptime(diz['TimeStamp'],'%Y-%m-%d %H:%M:%S')
                ###CANCEL SSTF IF PRESENT###
                if 'SSTF_mean' in diz:
                    del diz['SSTF_mean']

                publisher(diz)
                
            f.seek(0)
            json.dump(file_reader, f, indent = 4, default=str, ensure_ascii=False)
            
            #return storic_list_rivers  RETURN LIST OF RIVERS OBJECT

    def sort_rivers(lista_of_rivers:list): ### SORT BY TIMESTAMP
        lista_of_rivers = sorted(lista_of_rivers, key=lambda x: x['TimeStamp'])
        return lista_of_rivers

    def add_season(diz:dict):
        tempo = diz['TimeStamp']
        tempo = tempo.split() #
        data = tempo[0]  #2019-04-12
        mese_giorno = data[5:]
        if mese_giorno > '03-20' and  mese_giorno <= '06-20':
            diz['Stagione'] = 'Spring'
        elif mese_giorno > '06-20' and  mese_giorno <= '09-20':
            diz['Stagione'] = 'Summer'
        elif  mese_giorno > '09-20' and  mese_giorno <= '12-20':
            diz['Stagione'] = 'Autumn'
        else:
            diz['Stagione'] = 'Winter'
        return diz

    def add_id(diz:dict):
        if diz['NAME'] == "EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD": ###ISARCO
            diz['ID'] = 1
        elif diz['NAME'] == "ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE": ###ADIGE
            diz['ID'] = 2
        else:
            diz['ID'] = 3  ## TALVERA
        return diz

Manager_dati_storici.manage_dati_storici()