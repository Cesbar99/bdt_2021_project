import findspark
findspark.init('C:\spark-3.1.2-bin-hadoop3.2')
findspark.find()
import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
conf = pyspark.SparkConf().setAppName('SparkApp').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

from pyspark.sql.functions import udf
from pyspark.sql.types import *
import os
import csv
from mqtt_fiumi_publisher import publisher_str
import json
import requests
from urllib.request import urlopen

sqlContext.sql("set spark.sql.shuffle.partitions=6"); 

def from_scode_to_id(scode:str):
    
    if scode == "29850PG":
        return 2 #ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE
    elif scode == "83450PG":
        return 1 #EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD
    elif scode == "82910PG":
        return 3 #TALFER BEI BOZEN/TALVERA A BOLZANO 

def add_season(stringa):
    
    tempo = stringa.split() #
    data = tempo[0]  #2019-04-12
    mese_giorno = data[5:]
    if mese_giorno > '03-20' and  mese_giorno <= '06-20':
        stagione = 'Spring'
    elif mese_giorno > '06-20' and  mese_giorno <= '09-20':
        stagione = 'Summer'
    elif  mese_giorno > '09-20' and  mese_giorno <= '12-20':
        stagione = 'Autumn'
    else:
        stagione = 'Winter'
    return stagione

def from_scode_to_name(scode:str):
    
    if scode == "29850PG":
        return 'ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE'
    elif scode == "83450PG":
        return 'EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD'
    elif scode == "82910PG":
        return 'TALFER BEI BOZEN/TALVERA A BOLZANO'
    

url = 'http://dati.retecivica.bz.it/services/meteo/v1/sensors'

jsonData = urlopen(url).read().decode('utf-8')
rdd = spark.sparkContext.parallelize([jsonData])
df = spark.read.json(rdd)

df = df.filter( ( (df.SCODE == "29850PG") | (df.SCODE == "83450PG") | (df.SCODE == "82910PG") ) & (df.UNIT != "mg/l") )

udf_add_season = udf(add_season, StringType())
udf_id = udf(from_scode_to_id, IntegerType())

df = df.withColumn('Stagione', udf_add_season('DATE'))
df = df.withColumn('ID', udf_id('SCODE'))
df = df.drop('DESC_D')
df = df.drop('DESC_I')
df = df.drop('DESC_L')
df = df.drop('SCODE')
df = df.drop('UNIT')

df_1st = df.filter((df.ID == '1'))
df_2nd = df.filter((df.ID == '2'))
df_3rd = df.filter((df.ID == '3'))

inputs = [df_1st, df_2nd, df_3rd]

for i in range(len(inputs)):
    
    lista = inputs[i].head()
    TimeStamp = lista[0]
    Stagione = lista[3]
    Id = lista[4]
    valori = inputs[i].select("VALUE").rdd.flatMap(lambda x: x).collect()
    Q_mean = valori[0]
    W_mean = valori[1]
    WT_mean = valori[2]

    dataframe = spark.createDataFrame([(TimeStamp, WT_mean, W_mean, Q_mean, Stagione, Id)], ['TimeStamp', 'WT_mean', 'W_mean', 'Q_mean', 'Stagione', 'ID' ])
    dataframe.write.option("header",True).csv("test_folder", mode = 'append')

path = os.environ.get('my_path')
path = path + 'test_folder'
os.chdir(path)

for file in list(os.listdir()):
    if file[-3:] != 'csv':
        os.remove(file)

ids = []

for file in list(os.listdir()):
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        first_line = next(csv_reader)
        ids.append(int(first_line[-1]))
            
            
for i in range(len(ids)):
    if ids[i] == 1:
        os.rename(list(os.listdir())[i], 'created_csv_isarco.csv')
    elif ids[i] == 2:
        os.rename(list(os.listdir())[i], 'created_csv_adige.csv')
    else:
        os.rename(list(os.listdir())[i], 'created_csv_talvera.csv')

publisher_str('3 file creati! è ora di salvarli')

sc.stop()