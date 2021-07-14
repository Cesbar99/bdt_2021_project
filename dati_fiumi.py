
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

class raw_rivers:

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
    
    def save(self, debug = None) -> None:
        
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
            print( 'Rimosso il file: {file_name}'.format(file_name = files[i]) )
         
        cursor.close()

        print('Terminato con successo!')
        print('')

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

    def from_name_to_table(name:str, debug = None) -> str:

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
        queries = ["SHOW TABLES LIKE 'Tabella_Adige';", "SHOW TABLES LIKE 'Tabella_Isarco';", "SHOW TABLES LIKE 'Tabella_Talvera';", "SHOW TABLES LIKE 'Tabella_nomi';", "SHOW TABLES LIKE 'Try_Adige';","SHOW TABLES LIKE 'Try_Isarco';", "SHOW TABLES LIKE 'Try_Talvera';", "SHOW TABLES LIKE 'pred_Isarco_Q_mean';", "SHOW TABLES LIKE 'pred_Isarco_W_mean';", "SHOW TABLES LIKE 'pred_Isarco_WT_mean';", "SHOW TABLES LIKE 'pred_Adige_Q_mean';", "SHOW TABLES LIKE 'pred_Adige_W_mean';", "SHOW TABLES LIKE 'pred_Adige_WT_mean';", "SHOW TABLES LIKE 'pred_Talvera_Q_mean';", "SHOW TABLES LIKE 'pred_Talvera_W_mean';", "SHOW TABLES LIKE 'pred_Talvera_WT_mean';"]
        non_existing_tables = []
        for query in queries: 
            cursor.execute(query)
            output = cursor.fetchall()
            if len(output) == 0:
                non_existing_tables.append(query[18:-2])
                
        cursor.close()
        return non_existing_tables

    def create(self) -> None:
        
        cursor = self.connection.cursor()
        # table_name_fiumi = 'Tabella_fiumi' 
        nomi_tabelle = MYSQLRivers.check_tables_exist(self) #['Tabella_Adige', 'Tabella_Isarco', 'Tabella_Talvera']
        if len(nomi_tabelle) > 0:
            if 'Tabella_nomi' in nomi_tabelle:
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
                if 'Tabella' in table_name or 'Try' in table_name:
                    cursor.execute(MYSQLRivers.query_table(table_name))
                else:
                    #tabelle = ['pred_Isarco_Q_mean', 'pred_Isarco_W_mean', 'pred_Isarco_WT_mean', 'pred_Adige_Q_mean', 'pred_Adige_W_mean', 'pred_Adige_WT_mean', 'pred_Talvera_Q_mean', 'pred_Talvera_W_mean', 'pred_Talvera_WT_mean']
                    create_table_query = '''
                        CREATE TABLE {nome_tabella}
                        (
                        Timestamp DATETIME NOT NULL,
                        {var}_1h FLOAT (20,2) ,
                        {var}_3h FLOAT (20,2) ,
                        {var}_12h  FLOAT (20,2) ,
                        {var}_1d  FLOAT (20,2) ,
                        {var}_3d  FLOAT (20,2) ,
                        {var}_1w  FLOAT (20,2) ,
                        Id INT
                        )
                        '''.format(nome_tabella = table_name, var = table_name[-6:])

                    cursor.execute(create_table_query)
            
           
            cursor.close()
                
            print('All tables created, ready to get some data!')

        else:
            print('All tables already present, ready to get new data!')
        
        self.connection.close()

        
    
        


