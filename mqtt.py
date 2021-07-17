import os
import paho.mqtt.client as mqtt #import the client1
import time
from datetime import datetime
from Analysis_FINAL import make_predictions

from dati_fiumi import Rivers, MYSQLRivers

########################################

def on_message(client, userdata, message):

    if message.payload.decode() == '3 file creati! è ora di salvarli':
        manager.save(debug=False, new_observation=True) #debug = True
    elif message.payload.decode() == 'Dati storici in arrivo! è ora di salvarli':
        manager.save(debug=False)
    elif message.payload.decode() == 'Nuove osservazioni salvate, cosa ha in serbo il futuro per noi?':
        make_predictions()
    elif message.payload.decode() == 'Previsioni completate, salvale!':
        manager.save(prediction=True) #debug = True
    
########################################
    
lista_ricevuti = []
manager = MYSQLRivers()
broker_address= "broker.emqx.io" 
client = mqtt.Client('fiumi-storer') 
client.connect(broker_address, 1883, 60) 
client.subscribe(os.environ.get('topic')) 
client.on_message = on_message 
client.loop_forever()
