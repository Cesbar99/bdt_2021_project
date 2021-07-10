
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


from mqtt_fiumi_publisher import publisher_dic

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

# os.chdir('C:\Users\Cesare\OneDrive\studio\magistrale-data-science\big-data-tech\bdt_2021_project')

class Name:
    def __init__(self, scode:str):
        self.name = Name.from_scode_to_name(scode)

    def to_repr(self) ->str:
        return self.name

    def from_scode_to_name(scode:str):
        if scode == "29850PG":
            return 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE'
        elif scode == "83450PG":
            return 'EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD'
        elif scode == "82910PG":
            return 'TALFER BEI BOZEN/TALVERA A BOLZANO'
    
class ID:
    def __init__(self, scode:str):
        self.id = ID.from_scode_to_id(scode)

    def to_repr(self) -> int:
        return self.id

    def from_scode_to_id(scode:str):
        if scode == "29850PG":
            return 2 #ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE
        elif scode == "83450PG":
            return 1 #EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD
        elif scode == "82910PG":
            return 3 #TALFER BEI BOZEN/TALVERA A BOLZANO 


class Stagione:
    def __init__(self, data:str):
        self.Stagione = Stagione.from_date_to_season(data)
    
    def to_repr(self) -> str:
        return self.Stagione
    
    def from_date_to_season(data:str):
        data = data.split('T') 
        data = data[0]  #2019-04-12
        mese_giorno = data[5:]
        if mese_giorno > '03-20' and  mese_giorno <= '06-20':
            return 'Spring'
        elif mese_giorno > '06-20' and  mese_giorno <= '09-20':
            return 'Summer'
        elif mese_giorno > '09-20' and  mese_giorno <= '12-20':
            return 'Autumn'
        else:
            return 'Winter'


class Time_Stamp:
    def __init__(self, data:str):
        self.time_stamp = Time_Stamp.time_conversion(data)
    
    def to_repr(self) -> str:
        return self.time_stamp
    
    def time_conversion(data):
        data = data.split('T')
        date = str(data[0])
        time = str(data[1])[:-3]
        return date + ' ' + time

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

class raw_rivers:

###FUNCTIONS
    def __init__(self, ID:ID, NAME:Name, TimeStamp:str, TYPE:str, VALUE:int, Stagione:Stagione):
        self.ID = ID 
        self.NAME = NAME
        self.TYPE = TYPE
        self.TimeStamp = TimeStamp
        self.VALUE = VALUE
        self.Stagione = Stagione 

    def to_repr(self) -> dict:
        return{
            'ID': self.ID,
            'TYPE' : self.TYPE, 
            'TimeStamp': self.TimeStamp,
            'NAME': self.NAME,
            'VALUE': self.VALUE,
            'Stagione': self.Stagione
        }

    @staticmethod 
    def from_repr(raw_data: dict, Id: Optional[str] = None, stagione: Optional[str] = None) -> raw_rivers:
        if not id and "SCODE" not in raw_data:
            raise Exception('Can not build River model: ID information missing')

        if not stagione and 'DATE' not in raw_data:
            raise Exception('Can not build River model: season information missing')

        return raw_rivers(
            Id if Id else ID.to_repr(ID(raw_data['SCODE'])) ,
            Name.to_repr(Name(raw_data['SCODE'])),
            Time_Stamp.to_repr(Time_Stamp(raw_data['DATE'])), #datetime.strptime( Time_Stamp.to_repr(Time_Stamp(raw_data['DATE'])), '%Y-%m-%d %H:%M:%S' ),
            raw_data['TYPE'],
            raw_data['VALUE'],
            stagione if stagione else Stagione.to_repr(Stagione(raw_data['DATE']))
        )
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

###RIVER CLASS: OUTPUT 3 DICTIONARIES TO ADD TO HISTORICAL DATA

class Rivers:
    def __init__(self, TimeStamp:datetime, NAME:str, Stagione:str, ID:int, Q_mean:int, W_mean:int, WT_mean:int):
        self.NAME = NAME
        self.TimeStamp = TimeStamp
        self.Q_mean = Q_mean
        self.W_mean = W_mean
        self.WT_mean = WT_mean
        self.ID = ID
        self.Stagione = Stagione      
    
    def get_id(self):
        return self.ID
    
    def name(self):
        return self.NAME

    def timestamp(self):
        return self.TimeStamp

    def q_mean(self):
        return self.Q_mean

    def w_mean(self):
        return self.W_mean

    def wt_mean(self):
        return self.WT_mean

    def stagione(self):
        return self.Stagione

    def to_repr(self) -> dict:
        return {
            'TimeStamp': self.TimeStamp, 
            'NAME': self.NAME,
            'Q_mean': self.Q_mean,
            'W_mean': self.W_mean,
            'WT_mean': self.WT_mean,
            'Stagione': self.Stagione,
            'ID': self.ID
        }        

    @staticmethod 
    def from_repr(diz: dict) -> Rivers:
        if 'Q_mean' not in diz: 
            diz['Q_mean'] = '\\N' #None #math.nan

        if 'W_mean' not in diz: 
            diz['W_mean'] = '\\N' #None #math.nan

        if 'WT_mean' not in diz: 
            diz['WT_mean'] = '\\N' #None #math.nan
        
        return Rivers(
            datetime.strptime(diz['TimeStamp'], '%Y-%m-%d %H:%M:%S' ), #DATETIME OBJECT; if string needed use: diz['TimeStamp']
            diz['NAME'],
            diz['Stagione'],
            diz['ID'],
            diz['Q_mean'],
            diz['W_mean'], 
            diz['WT_mean'] 
        )


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

class manager_dati_nuovi:

    def __init__(self):
        self.dic1 = {'NAME':'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE'}
        self.dic2 = {'NAME':'TALFER BEI BOZEN/TALVERA A BOLZANO'}
        self.dic3 = {'NAME':'EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD'}
        self.insert1 = []
        self.insert2 = []
        self.insert3 = []
        self.discriminants_scode = ["29850PG", "83450PG", "82910PG"]
        self.discriminants_type = ["Q", "W", "WT"]

    def manage_new_rivers(self, url:str):
        response = requests.get(url)
        raw_fiumi = response.json()
        #destination_folder = 'test_folder'

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

    def transfer_json(self):
        self.insert1.append(self.dic1)
        self.insert2.append(self.dic2)
        self.insert3.append(self.dic3)
        with open("created_json_isarco.json", "w") as target1:
            json.dump(self.insert3, target1, default=str, ensure_ascii=False)
        with open("created_json_adige.json", "w") as target2:
            json.dump(self.insert1, target2, default=str, ensure_ascii=False)
        with open("created_json_talvera.json", "w") as target3:
            json.dump(self.insert2, target3, default=str, ensure_ascii=False)

    def from_json_to_csv(self):
        df = pd.read_json('created_json_talvera.json')
        print(df)
        del df['NAME']
        df = df[['TimeStamp','Q_mean', 'W_mean', 'WT_mean', 'Stagione', 'ID']]
        export_csv = df.to_csv('test_folder/created_csv_talvera.csv', index = None, header=True)
        os.remove('created_json_talvera.json')

        df = pd.read_json('created_json_isarco.json')
        print(df)
        del df['NAME']
        df = df[['TimeStamp','Q_mean', 'W_mean', 'WT_mean', 'Stagione', 'ID']]
        export_csv = df.to_csv('test_folder/created_csv_isarco.csv', index = None, header=True)
        os.remove('created_json_isarco.json')

        df = pd.read_json('created_json_adige.json')
        print(df)
        del df['NAME']
        df = df[['TimeStamp','Q_mean', 'W_mean', 'WT_mean', 'Stagione', 'ID']]
        export_csv = df.to_csv('test_folder/created_csv_adige.csv', index = None, header=True)
        os.remove('created_json_adige.json')

    def publish_new_rivers(self):
        
        publisher_dic(self.dic1)
        publisher_dic(self.dic2)
        publisher_dic(self.dic3)

        print(self.dic1)
        print(self.dic2)
        print(self.dic3)
        

class MYSQLRivers:
    
    def __init__(self)-> None:
        self.connection = mysql.connector.connect(
        host = os.environ.get('host'), #'ec2-18-117-169-228.us-east-2.compute.amazonaws.com', #'127.0.0.1'
        port =  3310,
        database = 'database_fiumi',  #'rivers_db'
        user = os.environ.get('user'), #root, user_new
        password = os.environ.get('password'), #password, passwordnew_user
        allow_local_infile = True
        )
        self.connection.autocommit = True

        query = 'SET GLOBAL interactive_timeout=5400;' #6000
        cursor = self.connection.cursor()
        cursor.execute(query)
        cursor.close()
    
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
        ######
        path = os.environ.get('my_path') #C:/Users/Cesare/OneDrive/studio/magistrale-data-science/big-data-tech/bdt_2021_project/'
        path = path + 'test_folder/'
        os.chdir(path)

        files = list(os.listdir())
        tabelle = []

        #debug: tabelle = ['Try_Isarco', 'Try_Adige', 'Try_Talvera']
        #no-debug: tabelle = ['Tabella_Isarco', 'Tabella_Adige', 'Tabella_Talvera']

        if debug: 
            for file in files:
                
                to_add = file[12:]
                to_add = to_add[:-4]
                to_add =  to_add[0].upper() + to_add[1:] 
                
                tabelle.append('Try_' + to_add)
        else:
            for file in files:
                
                to_add = file[12:]
                to_add = to_add[:-4]
                to_add =  to_add[0].upper() + to_add[1:] 
                
                tabelle.append('Tabella_' + to_add)
            
        #print(tabelle)
        #print(files)
        ######
        '''
        if debug: 
            tabelle = ['Try_Isarco', 'Try_Adige', 'Try_Talvera']
        else:
            tabelle = ['Tabella_Isarco', 'Tabella_Adige', 'Tabella_Talvera']
        files = ['created_csv_isarco.csv', 'created_csv_adige.csv', 'created_csv_talvera.csv']
        '''
        cursor = self.connection.cursor()
        query = 'SET GLOBAL local_infile=1;'
        cursor.execute(query)

        for i in range(len(tabelle)):
            query = """LOAD DATA LOCAL INFILE '{path_and_file_name}'
                    INTO TABLE {table_name}
                    FIELDS TERMINATED BY ','
                    ENCLOSED BY '"'
                    LINES TERMINATED BY '\n'
                    IGNORE 1 LINES; 
                """.format(path_and_file_name = path + files[i], table_name = tabelle[i]) #path + files[i]
            cursor.execute(query)
            print( 'Salvato il file: {file_name}!'.format(file_name = files[i]) )

            os.remove( path+'{file_name}'.format(file_name = files[i]) )
            '''
            if debug:
                nome_file = 'created_json_{name}.json'.format(name = tabelle[i][4:].lower())
            else:
                nome_file = 'created_json_{name}.json'.format(name = tabelle[i][8:].lower())
                '''
            #os.remove( path + nome_file )
            print( 'Rimosso il file: {file_name}'.format(file_name = files[i]) )
            #print( 'Rimosso il file: {file_name}'.format(file_name = nome_file ) )
        
        cursor.close()

        print('Terminato con successo!')
        print('')
    
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

    def query_table(nome_tabella:str) -> str:
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
        '''.format(table_name = nome_tabella)
        
        return create_table_query

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

    def from_name_to_table(name:str, debug = None) -> str:
        '''
        if name == "EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD":
            table_name = 'Tabella_Isarco'
        elif name == 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE':
            table_name = 'Tabella_Adige'
        else:
            table_name = 'Tabella_Talvera'
        return table_name'''

        if debug:
            if name == "EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD":
                table_name = 'Try_Isarco'
            elif name == 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE':
                table_name = 'Try_Adige'
            else:
                table_name = 'Try_Talvera'
            return table_name
        else:
            if name == "EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD":
                table_name = 'Tabella_Isarco'
            elif name == 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE':
                table_name = 'Tabella_Adige'
            else:
                table_name = 'Tabella_Talvera'
            return table_name

    def check_tables_exist(self):
        cursor = self.connection.cursor()
        queries = ["SHOW TABLES LIKE 'Tabella_Adige';", "SHOW TABLES LIKE 'Tabella_Isarco';", "SHOW TABLES LIKE 'Tabella_Talvera';", "SHOW TABLES LIKE 'Tabella_nomi';", "SHOW TABLES LIKE 'Try_Adige';","SHOW TABLES LIKE 'Try_Isarco';", "SHOW TABLES LIKE 'Try_Talvera';"]
        non_existing_tables = []
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

        cursor.close()
        return non_existing_tables

    def create(self) -> None:
        
        cursor = self.connection.cursor()
        # table_name_fiumi = 'Tabella_fiumi' 
        nomi_tabelle = MYSQLRivers.check_tables_exist(self) #['Tabella_Adige', 'Tabella_Isarco', 'Tabella_Talvera']
        if len(nomi_tabelle) > 0:
            if 'Tabella_nomi' in nomi_tabelle > 0:
                print('Creating table: Tabella_nomi')

                table_name = 'Tabella_nomi'
                create_table_names_query = '''
                CREATE TABLE {name} 
                (
                Id INT ,
                Name NVARCHAR(128) NOT NULL
                )
                '''.format(name = table_name)

                cursor.execute(create_table_names_query)
                nomi_tabelle.remove('Tabella_nomi')

                names = [" EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD", "ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE", "TALFER BEI BOZEN/TALVERA A BOLZANO"] 
                query = 'INSERT INTO Tabella_nomi(Id, Name) VALUES (%s, %s)'

                for i in range(1, 4):
                    cursor.execute(query, (i, names[i-1]) )

            for table_name in nomi_tabelle:
                print('Creating table: ', table_name)
                cursor.execute(MYSQLRivers.query_table(table_name))

            cursor.close()
                
            print('All tables created, ready to get some data!')

        else:
            print('All tables already present, ready to get new data!')
        
        self.connection.close()

        
    
        


