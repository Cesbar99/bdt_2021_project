from __future__ import absolute_import, annotations

import os
import json
import time
from datetime import datetime
from typing import List, Optional
import requests
import sqlite3
import textwrap

from mqtt_fiumi_publisher import publisher
from dati_fiumi import raw_rivers, Rivers

class Manager_dati_storici:

    def __init__(self, dizionario:dict):
        self.diz = dizionario

    def manage_historic_river(self):
                        #### STAGIONE #########
        self.diz = Manager_dati_storici.add_season(self.diz)

                        ###### ID #########
        self.diz = Manager_dati_storici.add_id(self.diz)

                        ###CANCEL SSTF IF PRESENT###
        if 'SSTF_mean' in self.diz:
            del self.diz['SSTF_mean']
        
    def publish_historic_river(self):
                        
        #publisher(self.diz)
        print(self.diz)

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


class manager_dati_nuovi:

    def __init__(self):
        self.dic1 = {'NAME':'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE'}
        self.dic2 = {'NAME':'TALFER BEI BOZEN/TALVERA A BOLZANO'}
        self.dic3 = {'NAME':'EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD'}
        self.discriminants_scode = ["29850PG", "83450PG", "82910PG"]
        self.iscriminants_type = ["Q", "W", "WT"]

    def manage_new_rivers(self, url:str):
        response = requests.get(url)
        raw_fiumi = response.json()

        for river in raw_fiumi:
            if river['SCODE'] in set(self.discriminants_scode) and river['TYPE'] in set(self.discriminants_type):
                river = raw_rivers.to_repr(raw_rivers.from_repr(river))
    
                if river['NAME'] == 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE':
                    if  not 'TimeStamp' in self.dic1:
                        self.dic1['TimeStamp'] = river['TimeStamp']
                    if not 'Stagione' in self.dic1 :
                        self.dic1['Stagione'] = river['Stagione']
                    if not 'ID' in self.dic1:
                        self.dic1['ID'] = river['ID']
                    self.dic1[river['TYPE']+'_mean'] = river['VALUE'] 

                elif river['NAME'] == 'TALFER BEI BOZEN/TALVERA A BOLZANO':
                    if  not 'TimeStamp' in self.dic2:
                        self.dic2['TimeStamp'] = river['TimeStamp']
                    if not 'Stagione' in self.dic2:
                        self.dic2['Stagione'] = river['Stagione']
                    if not 'ID' in self.dic2:
                        self.dic2['ID'] = river['ID']
                    self.dic2[river['TYPE']+'_mean'] = river['VALUE'] 
                
                else:
                    if  not 'TimeStamp' in self.dic3:
                        self.dic3['TimeStamp'] = river['TimeStamp']
                    if not 'Stagione' in self.dic3:
                        self.dic3['Stagione'] = river['Stagione']
                    if not 'ID' in self.dic3:
                        self.dic3['ID'] = river['ID']
                    self.dic3[river['TYPE']+'_mean'] = river['VALUE']

    def publish_new_rivers(self):
        publisher(self.dic1)
        publisher(self.dic2)
        publisher(self.dic3)


