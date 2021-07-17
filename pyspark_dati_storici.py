
import findspark
findspark.init('C:\spark-3.1.2-bin-hadoop3.2')
findspark.find()
import pyspark
#findspark.find()


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
from dati_fiumi import MYSQLRivers, manager_dati_nuovi

manager_mysql = MYSQLRivers()
manager_mysql.create()

#numeric_val = sc.parallelize([1,2,3,4])
#numeric_val.map(lambda x:x*x*x).collect()

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

def add_id(name):

    if name == "EISACK BEI BOZEN SÜD/ISARCO A BOLZANO SUD": ###ISARCO
        Id = 1
    elif name == "ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE": ###ADIGE
        Id = 2
    else:
        Id = 3  ## TALVERA
    return Id

def convert_null(value):
    if value == None:
        return '\\N'
    else:
        return value
                                                                                    
df = spark.read.json("historic_data.json")
#df.printSchema()
#df.show()

udf_add_season = udf(add_season, StringType())
udf_add_id = udf(add_id, IntegerType())
udf_convert_null = udf(convert_null)

df = df.withColumn('Stagione', udf_add_season('TimeStamp'))
df = df.withColumn('ID', udf_add_id('NAME'))

df = df.withColumn('WT_try', udf_convert_null('WT_mean'))
df = df.withColumn('W_try', udf_convert_null('W_mean'))
df = df.withColumn('Q_try', udf_convert_null('Q_mean'))
df = df.drop('Q_mean')
df = df.drop('W_mean')
df = df.drop('WT_mean')
df = df.withColumnRenamed('WT_try', 'WT_mean')
df = df.withColumnRenamed('W_try', 'W_mean')
df = df.withColumnRenamed('Q_try', 'Q_mean')

#df.printSchema()
df = df.select('TimeStamp', 'Q_mean', 'W_mean', 'WT_mean', 'Stagione', 'ID')


#df.show()

df.createOrReplaceTempView("df")

emptyRDD = spark.sparkContext.emptyRDD()

schema = StructType([
    StructField('ID', IntegerType()),
    StructField('TimeStamp', StringType(), nullable=True),
    StructField('Season', StringType(), True),
    StructField('WT_mean', FloatType(), nullable=True),
    StructField('W_mean', FloatType(), nullable=True),
    StructField('Q_mean', FloatType(), nullable=True)
  ])

df_1 = spark.createDataFrame(emptyRDD,schema)
df_2 = spark.createDataFrame(emptyRDD,schema)
df_3 = spark.createDataFrame(emptyRDD,schema)

df_1 = spark.sql("select * from df where ID = 1")
df_2 = spark.sql("select * from df where ID = 2")
df_3 = spark.sql("select * from df where ID = 3")

#df_1.printSchema()
#df_1.show()
#df_2.show()
#df_3.show()



path = os.environ.get('my_path') #C:/Users/Cesare/OneDrive/studio/magistrale-data-science/big-data-tech/bdt_2021_project/'
path = path + 'test_folder'
os.chdir(path)

#df_1.write.option("header",True).csv(path, mode = 'append')
#df_2.write.option("header",True).csv(path, mode = 'append')
#df_3.write.option("header",True).csv(path, mode = 'append')

df_1.toPandas().to_csv('testo1.csv', index = False)
df_2.toPandas().to_csv('testo2.csv',index = False)
df_3.toPandas().to_csv('testo3.csv',index = False)
'''

for file in list(os.listdir()):
    if file[-3:] != 'csv':
        os.remove(file)
'''
ids = []

for file in list(os.listdir()):
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        csv_headings = next(csv_reader)
        first_line = next(csv_reader)
        ids.append(int(first_line[-1]))
            
            
for i in range(len(ids)):
    if ids[i] == 1:
        os.rename(list(os.listdir())[i], 'created_csv_isarco.csv')
    elif ids[i] == 2:
        os.rename(list(os.listdir())[i], 'created_csv_adige.csv')
    else:
        os.rename(list(os.listdir())[i], 'created_csv_talvera.csv')
        
#print(list(os.listdir()))



publisher_str('Dati storici in arrivo! è ora di salvarli')

sc.stop()