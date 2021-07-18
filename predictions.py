from mqtt_fiumi_publisher import publisher_str
from datetime import datetime
import dati_fiumi
import time
import joblib
import os

manager = dati_fiumi.MYSQLRivers()

nomi_modelli = ['Tabella_Adige-Q_mean_model', 'Tabella_Adige-W_mean_model', 'Tabella_Adige-WT_mean_model', 'Tabella_Isarco-Q_mean_model', 'Tabella_Isarco-W_mean_model', 'Tabella_Isarco-WT_mean_model', 'Tabella_Talvera-Q_mean_model', 'Tabella_Talvera-W_mean_model','Tabella_Talvera-WT_mean_model']
lista_modelli = []

path = os.environ.get('my_path')  
#path = 'E/'
for modello in lista_modelli:
    filename = path  + modello
    results = joblib.load(filename)
    lista_modelli.append(results)

cursor = manager.connection.cursor()
query = 'select Timestamp from Tabella_Adige ORDER BY Timestamp DESC LIMIT 1;'
cursor.execute(query)
output = cursor.fetchall()
last_datetime =output[0][0]
cursor.close()
print(last_datetime)

while True:

    cursor = manager.connection.cursor()
    query = 'select Timestamp from Tabella_Adige ORDER BY Timestamp DESC LIMIT 1;'
    cursor.execute(query)
    output = cursor.fetchall()
    #print(output[0])
    current_date =output[0][0]
    cursor.close()

    if current_date > last_datetime:
        manager.make_predictions(lista_modelli)
        last_datetime = current_date

    else:
        print('no new observations')
        #print(current_date)
    
    time.sleep(60)
