###Dati_fiumi
from __future__ import absolute_import, annotations
import mysql.connector
from mysql.connector import connection
import os
import json
import time
from datetime import datetime
from typing import List, Optional
from mysql.connector import cursor
import requests
import sqlite3
import textwrap
import math
import pandas as pd
###
###rivers_operation
from __future__ import absolute_import, annotations

import textwrap
import os
import json

from datetime import datetime
from typing import List, Optional
import requests
import pandas as pd
###
''' DA AGGIUNGERE AI DATI STORICI
id, stagione
'''
''' MODELLO
{TimeStamp: '2019-07-05 16:00:00', 'NAME': 'EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD', 'Q_mean': 149.67, 'W_mean': 208.33, 'WT_mean': 14.28, 'stagione': 'Estate'}
'''
'''
SCODES
ADIGE A OPONTE ADIGE { "SCODE" : "29850PG" }
ISARCO A BOLZANO SUD { "SCODE": "83450PG" }
TALVERA A BOLZANO { "SCODE":"82910PG" }
'''

'''d1 = dict()
d1['NAME'] = 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE'
d3 = dict()
d3['NAME'] = 'TALFER BEI BOZEN/TALVERA A BOLZANO'
d2 = dict()
d2['NAME'] = 'EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD'

new_rivers = [d1, d2, d3]
        
print(new_rivers)
'''

###INPUT EXAMPLE
'''
    {
"SCODE":"29850PG",
"TYPE":"Q",
"DESC_D":"Durchfluss",
"DESC_I":"Portata",
"DESC_L":"Ega passeda",
"UNIT":"m³/s",
"DATE":"2021-04-21T15:10:00CEST",
"VALUE":32.7
}
'''

from mqtt_fiumi_publisher import publisher_dic

# os.chdir('C:\Users\Cesare\OneDrive\studio\magistrale-data-science\big-data-tech\bdt_2021_project')

#C:/Users/Cesare/OneDrive/studio/magistrale-data-science/big-data-tech/bdt_2021_project/'

'''
        class shrink:
            def __init__(self, lista_9:list, name:str):
                self.diz_type_val = shrink.from_9_to_3(lista_9, name)
            
            def to_repr(self) -> dict:
                return self.diz_type_val
        
            def from_9_to_3(lista_9:list, name:str):
                diz_shrink = dict()
                diz_shrink['NAME'] = name
                for diz in lista_9:
                    if diz['NAME'] == name:
                        diz_shrink[diz['TYPE']+'_mean'] = diz['VALUE'] 
                        diz_shrink['ID'] = diz['ID']
                diz_shrink['TimeStamp'] = diz['TimeStamp']
                diz_shrink['Stagione'] = diz['Stagione']
                return diz_shrink   
        '''


###FUNZIONE GET
###URL: 'http://dati.retecivica.bz.it/services/meteo/v1/sensors'
'''
def get_rivers(url:str):
    response = requests.get(url)
    raw_fiumi = response.json()
    discriminants_scode = ["29850PG", "83450PG", "82910PG"]
    discriminants_type = ["Q", "W", "WT"]
    shrunk_rivers = []
    names = ['ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE', 'TALFER BEI BOZEN/TALVERA A BOLZANO', 'EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD']
    raw_rivers_list = [raw_rivers.from_repr(raw_fiume) for raw_fiume in raw_fiumi if raw_fiume['SCODE'] in set(discriminants_scode) and raw_fiume['TYPE'] in set(discriminants_type)]   
    list_represented_raw_rivers = [raw_rivers.to_repr(raw_river) for raw_river in raw_rivers_list]
    for name in names:
        shrunk_rivers.append( shrink.to_repr(shrink(list_represented_raw_rivers,name))  )
    #list_of_rivers = [Rivers.from_repr(shrunk_river) for shrunk_river in shrunk_rivers]
    #return list_of_rivers


    
    return shrunk_rivers
'''
'''
class Manager_dati_storici:
    def manage_dati_storici():
        with open('dati_for_tentativo.json', 'r+', encoding = 'utf-8') as f:
            file_reader = json.load(f)
            storic_list_rivers = []
            for diz in file_reader:
                        #### STAGIONI #########
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
                        ###### ID #########
                if diz['NAME'] == "EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD": ###ISARCO
                    diz['ID'] = 1
                elif diz['NAME'] == "ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE": ###ADIGE
                    diz['ID'] = 2
                else:
                    diz['ID'] = 3  ## TALVERA
                ###CONVERT DSTRING OF DATE IN DATE OBJECT
                diz['TimeStamp'] = datetime.strptime(diz['TimeStamp'],'%Y-%m-%d %H:%M:%S')
                ###CANCEL SSTF IF PRESENT###
                if 'SSTF_mean' in diz:
                    del diz['SSTF_mean']

                storic_list_rivers.append(Rivers.from_repr(diz))

            f.seek(0)
            json.dump(file_reader, f, indent = 4, default=str, ensure_ascii=False)
            storic_list_rivers = Manager_dati_storici.sort_rivers(storic_list_rivers)
            return storic_list_rivers ### RETURN LIST OF RIVERS OBJECT

    def sort_rivers(lista_of_rivers): ### SORT BY TIMESTAMP
        lista_of_rivers = sorted(lista_of_rivers, key=lambda river: river.timestamp())
        return lista_of_rivers
'''

def publish_new_rivers(self):
        
    publisher_dic(self.dic1)
    publisher_dic(self.dic2)
    publisher_dic(self.dic3)

    print(self.dic1)
    print(self.dic2)
    print(self.dic3)

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
        publisher_dic(self.diz)
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

        if diz['NAME'] == "EISACK BEI BOZEN S\u00c3\u0153D/ISARCO A BOLZANO SUD": ###ISARCO
            diz['ID'] = 1
        elif diz['NAME'] == "ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE": ###ADIGE
            diz['ID'] = 2
        else:
            diz['ID'] = 3  ## TALVERA
        return diz

#def save(self, river:Rivers) -> None:
#def save(self, lista_ricevuti:list, debug = None) -> None:
def save(self, debug = None) -> None:
        '''
        cursor = self.connection.cursor()

        table_name = MYSQLRivers.from_name_to_table(river.name())
        query = MYSQLRivers.query_insert(table_name)    
        cursor.execute(query, (river.get_id(), river.q_mean(), river.w_mean(), river.wt_mean(), river.timestamp(), river.stagione() ))
    
        cursor.close()
        #self.connection.close() 
        '''
        #######
        '''
        cursor = self.connection.cursor()

        if debug:
            i = 0
            while i < len(lista_ricevuti):
                river = lista_ricevuti[i]
                table_name = MYSQLRivers.from_name_to_table(river.name(), debug = True)
                query = MYSQLRivers.query_insert(table_name)    
                cursor.execute(query, (river.get_id(), river.q_mean(), river.w_mean(), river.wt_mean(), river.timestamp(), river.stagione() ))
                print('Un fiume salvato!')
                i += 1
            lista_ricevuti.clear()

        else:
            i = 0
            while i < len(lista_ricevuti):
                river = lista_ricevuti[i]
                table_name = MYSQLRivers.from_name_to_table(river.name())
                query = MYSQLRivers.query_insert(table_name)    
                cursor.execute(query, (river.get_id(), river.q_mean(), river.w_mean(), river.wt_mean(), river.timestamp(), river.stagione() ))
                print('Un fiume salvato!')
                i += 1
            lista_ricevuti.clear()

        cursor.close()
        #self.connection.close()
        '''

def from_db_to_list(self, table_name) -> List[Rivers]:
        cursor = self.connection.cursor()

        query1 = 'SELECT * from {tabella}'.format(tabella = table_name)

        cursor.execute(query1)
        rows = cursor.fetchall()

        rivers = []
        for Id, Q_mean, W_mean, WT_mean,Timestamp, Stagione in rows:
            query2 = 'SELECT Name FROM Tabella_nomi WHERE Id = {id}'.format(id = Id)
            cursor.execute(query2)
            name = cursor.fetchall()[0][0]
            rivers.append(
                Rivers(Timestamp, name , Stagione, Id, Q_mean, W_mean, WT_mean) 
            )
        
        cursor.close()
        self.connection.close()

        return rivers

def query_insert(nome_tabella:str) -> str:
    insert_query = '''
    INSERT into {table_name}(Id, Q_mean, W_mean, WT_mean, Timestamp, Stagione) VALUES (%s,%s,%s,%s,%s,%s)
    '''.format(table_name = nome_tabella)

    return insert_query


def from_table_to_name(table_name):

    if table_name == 'Tabella_Isarco':
        name = "EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD"
    elif table_name == 'Tabella_Adige':
        name = 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE'
    else:
        name = "TALFER BEI BOZEN/TALVERA A BOLZANO"
    return name

'''
if name == "EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD":
    table_name = 'Tabella_Isarco'
elif name == 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE':
    table_name = 'Tabella_Adige'
else:
    table_name = 'Tabella_Talvera'
return table_name'''


'''
def on_message(client, userdata, message):
    #print("message received " ,str(message.payload.decode("utf-8")))
    print('message received')
    print(message.payload.decode())
    dic = eval(message.payload.decode())
    print(dic['ID'], dic['NAME'])
    #print(dict(message.payload.decode())['Albergo'])
########################################
#broker_address="broker.hivemq.com"
broker_address="mqtt.eclipseprojects.io" #"iot.eclipse.org"
client = mqtt.Client('fiumi-storer') #client = mqtt.Client() create new instance ; client = mqtt.Client()
client.on_message=on_message #attach function to callback
client.connect(broker_address) #connect to broker
#client.loop_start() #start the loop
client.subscribe('fiumi') #client.subscribe('testtopic/#')
#client.loop_stop()
client.loop_forever()
#time.sleep(10) # wait
'''
'''
def on_message(client, userdata, message):
    #print("message received " ,str(message.payload.decode("utf-8")))
    #print('message received')
    #print(message.payload.decode())
    
    dic = eval(message.payload.decode())
    print(dic)
    river = Rivers.from_repr(dic)
    #print(Rivers.to_repr(river))
    manager.save(river)
    print('Un fiume salvato!')
'''

def on_message(client, userdata, message):
    #print("message received " ,str(message.payload.decode("utf-8")))
    #print('message received')
    #print(message.payload.decode())
    if message.payload.decode() == 'Dati terminati! Ricrodati di salvarli':
        manager.save(lista_ricevuti, debug = True) 
        #print(message.payload.decode())
    elif message.payload.decode() == '3 file creati! è ora di salvarli':
        manager.save(debug=True) #debug = True
    else:
        dic = eval(message.payload.decode())
        print(dic)
        river = Rivers.from_repr(dic)
        print(Rivers.to_repr(river))
        lista_ricevuti.append(river)
        print('Un fiume ricevuto!')

#broker_address= "broker.hivemq.com"
print(os.environ.get('my_path'))
lista_ricevuti = []
manager = MYSQLRivers()
broker_address= "broker.emqx.io" #"iot.eclipse.org"," "broker.emqx.io", "mqtt.eclipse.org"
client = mqtt.Client('fiumi-storer') #client = mqtt.Client() create new instance ; client = mqtt.Client()
client.connect(broker_address, 1883, 60) #connect to broker
client.subscribe(os.environ.get('topic')) #client.subscribe('testtopic/#')
client.on_message = on_message #attach function to callback
client.loop_forever()
#client.loop_start() #start the loop
#client.loop_stop()
#time.sleep(3600) # wait

'''
client = mqtt.Client(client_id = 'station-storer') #create a new client
#client.usarename_ow_set('bdt-2021', '') #the broaker is protected by an authentication 
client.connect('broker.emqx.io') #define the host. 
while True:
    client.publish('bdt-2021/test', 'this is a test') # I can decide the Qos and if retain or not the message.

    time.sleep(5)
'''
'''
client = mqtt.Client(client_id = 'fiumi-sender') #create a new client
#client.usarename_ow_set('bdt-2021', '') #the broaker is protected by an authentication 
client.connect("mqtt.eclipseprojects.io") #define the host. 
while True:
    dic = {'casa':'Trento', 'Albergo': 'Napoli'}
    client.publish('fiumi', str(dic)) # I can decide the Qos and if retain or not the message.
    time.sleep(5)
'''

def publisher_dic(dic:dict):
    client = mqtt.Client(client_id = 'fiumi-sender') #create a new client
    #client.usarename_ow_set('bdt-2021', '') #the broaker is protected by an authentication 
    client.connect("broker.emqx.io", 1883, 60) #define the host. #alternatively "broker.emqx.io" "mqtt.eclipse.org"
    client.publish(os.environ.get('topic'), str(dic)) # I can decide the Qos and if retain or not the message.
    
#client.username_ow_set('bdt-2021', '') #the broaker is protected by an authentication 

for i in range(len(queries)):
            cursor.execute(queries[i])
            output = cursor.fetchall()
            if len(output) == 0:
                if i == 0:
                    print('Tabella Adige not existing')
                    non_existing_tables.append('Tabella_Adige')
                if i == 1:
                    print('Tabella Isarco not existing')
                    non_existing_tables.append('Tabella_Isarco')
                if i == 2:
                    print('Tabella Talvera not existing')
                    non_existing_tables.append('Tabella_Talvera')
                if i == 3:
                    print('Tabella nomi not existing')
                    non_existing_tables.append('Tabella_nomi')
                if i == 4:
                    print('Debug table for river Adige not existing')
                    non_existing_tables.append('Try_Adige')
                if i == 5:
                    print('Debug table for river Isarco not existing')
                    non_existing_tables.append('Try_Isarco')
                if i == 6:
                    print('Debug table for river Talvera not existing')
                    non_existing_tables.append('Try_Talvera')
     