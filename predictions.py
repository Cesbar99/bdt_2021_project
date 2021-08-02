from mqtt_fiumi_publisher import publisher_str
from datetime import datetime
import dati_fiumi
import time
import joblib
import os
#from compute_models import model1, model2, model3, model4, model5, model6, model7, model8, model9
#from Analysis_FINAL import make_predictions
#print('starting')
manager = dati_fiumi.MYSQLRivers()
cursor = manager.connection.cursor()
query = 'select Timestamp from Tabella_Adige ORDER BY Timestamp DESC LIMIT 1;'
cursor.execute(query)
output = cursor.fetchall()
last_datetime_Adige =output[0][0]
query = 'select Timestamp from Tabella_Isarco ORDER BY Timestamp DESC LIMIT 1;'
cursor.execute(query)
output = cursor.fetchall()
last_datetime_Isarco =output[0][0]
query = 'select Timestamp from Tabella_Talvera ORDER BY Timestamp DESC LIMIT 1;'
cursor.execute(query)
output = cursor.fetchall()
last_datetime_Talvera =output[0][0]
cursor.close()
#print(last_datetime)

while True:
    
    pred = False
    cursor = manager.connection.cursor()

    query = 'select Timestamp from Tabella_Adige ORDER BY Timestamp DESC LIMIT 1;'
    cursor.execute(query)
    output = cursor.fetchall()
    current_date_adige =output[0][0]
    

    if current_date_adige > last_datetime_Adige:
        manager.make_predictions('Adige')
        print('computing')
        last_datetime_Adige = current_date_adige
        pred = True
        print('done')

    else:
        print('no new observations for Adige')

    query = 'select Timestamp from Tabella_Isarco ORDER BY Timestamp DESC LIMIT 1;'
    cursor.execute(query)
    output = cursor.fetchall()
    current_date_isarco =output[0][0]
    

    if current_date_isarco > last_datetime_Isarco:
        manager.make_predictions('Isarco')
        print('computing')
        last_datetime_Isarco = current_date_isarco
        pred = True
        print('done')

    else:
        print('no new observations for Isarco')    

    query = 'select Timestamp from Tabella_Talvera ORDER BY Timestamp DESC LIMIT 1;'
    cursor.execute(query)
    output = cursor.fetchall()
    current_date_talvera =output[0][0]
    
    if current_date_talvera > last_datetime_Talvera:
        manager.make_predictions('Talvera')
        print('computing')
        last_datetime_Talvera = current_date_talvera
        pred = True
        print('done')

    else:
        print('no new observations for Talvera')

    cursor.close()

    if pred:
        publisher_str('Previsioni completate, salvale!')
    
    time.sleep(60)
