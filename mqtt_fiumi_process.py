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
from dati_fiumi import raw_rivers

class Manager_dati_storici:
    def manage_dati_storici():
        with open('try_data.json', 'r+', encoding = 'utf-8') as f: #substitute with real json with all historical data
            file_reader = json.load(f)
            file_reader = Manager_dati_storici.sort_rivers(file_reader)
            for diz in file_reader:

                        #### STAGIONI #########
                diz = Manager_dati_storici.add_season(diz)

                        ###### ID #########
                diz = Manager_dati_storici.add_id(diz)

                        ###CANCEL SSTF IF PRESENT###
                if 'SSTF_mean' in diz:
                    del diz['SSTF_mean']

                        ###PUBLISH DIZ WITH MQTTX###
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

def get_those_rivers(url:str):
    response = requests.get(url)
    raw_fiumi = response.json()
    discriminants_scode = ["29850PG", "83450PG", "82910PG"]
    discriminants_type = ["Q", "W", "WT"]
    dic1 = dict()
    dic2 = dict()
    dic3 = dict()
    dic1['NAME'] = 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE'
    dic2['NAME'] = 'TALFER BEI BOZEN/TALVERA A BOLZANO'
    dic3['NAME'] = 'EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD'
    for river in raw_fiumi:
        if river['SCODE'] in discriminants_scode and river['TYPE'] in discriminants_type:
            river = raw_rivers.to_repr(raw_rivers.from_repr(river))

            if river['NAME'] == 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE':
                if  not 'TimeStamp' in dic1:
                    dic1['TimeStamp'] = river['TimeStamp']
                if not 'Stagione' in dic1 :
                    dic1['Stagione'] = river['Stagione']
                if not 'ID' in dic1:
                    dic1['ID'] = river['ID']
                dic1[river['TYPE']+'_mean'] = river['VALUE'] 

            elif river['NAME'] == 'TALFER BEI BOZEN/TALVERA A BOLZANO':
                if  not 'TimeStamp' in dic2:
                    dic2['TimeStamp'] = river['TimeStamp']
                if not 'Stagione' in dic2:
                    dic2['Stagione'] = river['Stagione']
                if not 'ID' in dic2:
                    dic2['ID'] = river['ID']
                dic2[river['TYPE']+'_mean'] = river['VALUE'] 
            
            else:
            #elif river['NAME'] == 'EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD':
                if  not 'TimeStamp' in dic3:
                    dic3['TimeStamp'] = river['TimeStamp']
                if not 'Stagione' in dic3:
                    dic3['Stagione'] = river['Stagione']
                if not 'ID' in dic3:
                    dic3['ID'] = river['ID']
                dic3[river['TYPE']+'_mean'] = river['VALUE']

    publisher(dic1)
    publisher(dic2)
    publisher(dic3)

Manager_dati_storici.manage_dati_storici()
get_those_rivers('http://dati.retecivica.bz.it/services/meteo/v1/sensors')

