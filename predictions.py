
import dati_fiumi
import time

#from compute_models import model1, model2, model3, model4, model5, model6, model7, model8, model9
#from Analysis_FINAL import make_predictions
#print('starting')
manager = dati_fiumi.MYSQLRivers()
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
    current_date =output[0][0]
    cursor.close()

    if current_date > last_datetime:
        manager.make_predictions()
        print('computing')
        last_datetime = current_date
        print('done')

    else:
        print('no new observations')
    
    time.sleep(60)
