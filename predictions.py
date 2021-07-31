import dati_fiumi
import time

#from compute_models import model1, model2, model3, model4, model5, model6, model7, model8, model9
#from Analysis_FINAL import make_predictions
#print('starting')
manager = dati_fiumi.MYSQLRivers()
cursor = manager.connection.cursor()
query_adige = 'select Timestamp from Tabella_Adige ORDER BY Timestamp DESC LIMIT 1;'
cursor.execute(query_adige)
output = cursor.fetchall()
last_datetime_adige =output[0][0]
query_Isarco = 'select Timestamp from Tabella_Isarco ORDER BY Timestamp DESC LIMIT 1;'
cursor.execute(query_Isarco)
output = cursor.fetchall()
last_datetime_Isarco =output[0][0]
query_Talvera = 'select Timestamp from Tabella_Talvera ORDER BY Timestamp DESC LIMIT 1;'
cursor.execute(query_Talvera)
output = cursor.fetchall()
last_datetime_Talvera =output[0][0]
cursor.close()
manager.connection.close()
while True:
    manager =  dati_fiumi.MYSQLRivers()
    cursor = manager.connection.cursor()
    query_Adige = 'select Timestamp from Tabella_Adige ORDER BY Timestamp DESC LIMIT 1;'
    query_Isarco = 'select Timestamp from Tabella_Isarco ORDER BY Timestamp DESC LIMIT 1;'
    query_Talvera = 'select Timestamp from Tabella_Talvera ORDER BY Timestamp DESC LIMIT 1;'
    cursor.execute(query_Adige)
    output = cursor.fetchall()
    current_date_adige =output[0][0]
    
    #cursor.close()
    if current_date_adige > last_datetime_adige:
        manager.make_predictions('Tabella_Adige')
        print('computing')
        last_datetime_adige = current_date_adige
        print('done')

    else:
        print('no new observations Adige')
    
    cursor.execute(query_Isarco)
    output = cursor.fetchall()
    current_date_isarco  =output[0][0]
    
    if current_date_isarco > last_datetime_Isarco:
        manager.make_predictions('Tabella_Isarco')
        print('computing')
        last_datetime_Isarco = current_date_isarco
        print('done')

    else:
        print('no new observations Isarco')
    
    cursor.execute(query_Talvera)
    output = cursor.fetchall()
    current_date_talvera =output[0][0]
    
    if current_date_talvera > last_datetime_Talvera:
        manager.make_predictions('Tabella_Talvera')
        print('computing')
        last_datetime_Talvera = current_date_talvera
        print('done')

    else:
        print('no new observations Talvera')
    
    manager.connection.close()
    time.sleep(60)
