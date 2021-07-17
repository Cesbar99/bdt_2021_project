from mqtt_fiumi_publisher import publisher_str
from datetime import datetime
import dati_fiumi
import time

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
    #print(output[0])
    current_date =output[0][0]
    cursor.close()

    if current_date > last_datetime:
        manager.make_predictions()
        last_datetime = current_date

    else:
        print('no new observations')
        #print(current_date)
    
    time.sleep(60)
