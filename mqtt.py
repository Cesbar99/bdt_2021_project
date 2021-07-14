import os
import paho.mqtt.client as mqtt #import the client1
import time
from datetime import datetime

from dati_fiumi import Rivers, MYSQLRivers

########################################

def on_message(client, userdata, message):

    if message.payload.decode() == '3 file creati! è ora di salvarli':
        manager.save(debug=False) #debug = True
    
########################################
    
lista_ricevuti = []
manager = MYSQLRivers()
broker_address= "broker.emqx.io" 
client = mqtt.Client('fiumi-storer') 
client.connect(broker_address, 1883, 60) 
client.subscribe(os.environ.get('topic')) 
client.on_message = on_message 
client.loop_forever()
