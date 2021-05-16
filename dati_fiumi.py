
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

os.chdir('C:/Users/Cesare/OneDrive/studio/magistrale-data science/big data tech')

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
    def __init__(self, TimeStamp:str, NAME:str, Stagione:str, ID:int, Q_mean:int, W_mean:int, WT_mean:int):
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

        return Rivers(
            diz['TimeStamp'], #Transform in date object
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

class SQLAzureRivers:
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
    
    def query_table(nome_tabella:str) -> str:
        create_table_query = '''
        CREATE TABLE {table_name}
        (
            Id INT ,
            Q_mean NUMERIC NOT NULL,
            W_mean NUMERIC NOT NULL,
            WT_mean  NUMERIC NOT NULL,
            Timestamp DATETIME NOT NULL,
            Stagione NVARCHAR(128) NOT NULL
        )
        '''.format(table_name = nome_tabella)

        return create_table_query

    def query_insert(nome_tabella:str) -> str:
        insert_query = '''
        INSERT into {table_name}(id, Q_mean, W_mean, WT_mean,Timestamp, Stagione) VALUES (?,?,?,?,?,?)
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

    def from_name_to_table(name:str) -> str:

        if name == "EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD":
            table_name = 'Tabella_Isarco'
        elif name == 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE':
            table_name = 'Tabella_Adige'
        else:
            table_name = 'Tabella_Talvera'
        return table_name

    def create(self) -> None:

        #table_name_fiumi = 'Tabella_fiumi' 
        nomi_tabelle = ['Tabella_Adige', 'Tabella_Isarco','Tabella_Talvera']

        table_names = 'Tabella_nomi'
        create_table_names_query = '''
        CREATE TABLE {table_name}
        (
            Id INT ,
            Name NVARCHAR(128) NOT NULL,
        )
        '''.format(table_name = table_names)

        cnxn: pyodbc.Connection = pyodbc.connect(self.connection_string)
        crsr: pyodbc.Cursor = cnxn.cursor()

        for table_name in nomi_tabelle:
            crsr.execute(SQLAzureRivers.query_table(table_name))
            crsr.commit()
            
        crsr.execute(create_table_names_query)
        crsr.commit()

        names = [" EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD", "ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE", "TALFER BEI BOZEN/TALVERA A BOLZANO"] 
        query = 'INSERT INTO Tabella_nomi(id, Name) VALUES (?, ?)'

        for i in range(1, 4):
            crsr.execute(query, (i, names[i-1]) )
            crsr.commit()

        cnxn.close()
    '''
    def save(self, rivers:List[Rivers]) -> None:

        cnxn: pyodbc.Connection = pyodbc.connect(self.connection_string)
        crsr: pyodbc.Cursor = cnxn.cursor()

        for river in rivers:
            table_name = SQLAzureRivers.from_name_to_table(river.name())
            query = SQLAzureRivers.query_insert(table_name)

            #crsr.execute('SET IDENTITY_INSERT Fiumi_try ON')
            crsr.execute(query, (river.get_id(), river.q_mean(), river.w_mean(), river.wt_mean(), river.timestamp(), river.stagione() ))
            crsr.commit()

        cnxn.close()
    '''

    def save(self, river:Rivers) -> None:

        cnxn: pyodbc.Connection = pyodbc.connect(self.connection_string)
        crsr: pyodbc.Cursor = cnxn.cursor()

        table_name = SQLAzureRivers.from_name_to_table(river.name())
        query = SQLAzureRivers.query_insert(table_name)

        #crsr.execute('SET IDENTITY_INSERT Fiumi_try ON')
        crsr.execute(query, (river.get_id(), river.q_mean(), river.w_mean(), river.wt_mean(), river.timestamp(), river.stagione() ))
        crsr.commit()

        cnxn.close()
        
    def from_db_to_list(self, table_name) -> List[Rivers]:
        cnxn: pyodbc.Connection = pyodbc.connect(self.connection_string)
        crsr: pyodbc.Cursor = cnxn.cursor()

        query1 = 'SELECT * from {tabella}'.format(tabella = table_name)

        crsr.execute(query1)
        rows = crsr.fetchall()

        rivers = []
        for Id, Q_mean, W_mean, WT_mean,Timestamp, Stagione in rows:
            query2 = 'SELECT Name FROM Tabella_nomi WHERE Id = {id}'.format(id = Id)
            crsr.execute(query2)
            name = crsr.fetchall()[0][0]
            rivers.append(
                Rivers(Timestamp, name , Stagione, Id, Q_mean, W_mean, WT_mean) 
            )
        
        cnxn.close()

        return rivers

'''
class RiverManager:

    RIVERS_FILE = "rivers_file.json" ###MUST HAVE SAME NAME THAN FILE WITH HISTORICAL DATA
    
    def __init__(self) -> None:
        if not os.path.isfile(self.RIVERS_FILE):
            with open("rivers_file.json", "w") as f:
                json.dump([], f)
    
    def save(self, rivers: List[Rivers]) -> None:
        old_rivers = self.to_list()
        update_rivers = old_rivers + rivers

        with open("rivers_file.json", "w") as f:
            json.dump(
                [Rivers.to_repr(river) for river in update_rivers],
                f,
                indent=4,
                default=str,
                ensure_ascii=False
            )

    def to_list(self) -> List[Rivers]:
        with open("rivers_file.json", "r") as f:
            fiumi = json.load(f)
            return [Rivers.from_repr(fiume) for fiume in fiumi]
'''

'''
class SQLliteRiverManager:
    DB_NAME = 'C:/Users/Cesare/OneDrive/studio/magistrale-data science/big data tech/database_fiumi.db' 

    def save(self, rivers: List[Rivers]) -> None:
        conn = sqlite3.connect('C:/Users/Cesare/OneDrive/studio/magistrale-data science/big data tech/database_fiumi.db')
        cursor = conn.cursor()
        query = 'INSERT into Fiumi (id, Name, Q_mean, W_mean, WT_mean,Timestamp, Stagione) VALUES (?,?,?,?,?,?,?)'
        
        for river in rivers:
            cursor.execute(query, (river.get_id(), river.name(), river.q_mean(), river.w_mean(), river.wt_mean(), river.timestamp(), river.stagione() ))
            conn.commit()
        
        conn.close()

    def list(self) -> List[Rivers]:
        conn = sqlite3.connect(self.DB_NAME)
        cursor = conn.cursor()
        query = 'SELECT * from Fiumi;'
        cursor.execute(query)
        rows = cursor.fetchall()

        rivers = []
        for id, Name, Q_mean, W_mean, WT_mean,Timestamp, Stagione in rows:
            rivers.append(
                Rivers(id, Name, Q_mean, W_mean, WT_mean,datetime.fromisoformat(Timestamp), Stagione)
            )
        
        conn.close()

        return rivers
'''

