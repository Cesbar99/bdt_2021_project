import os
from paho.mqtt import client as mqtt
import time
from datetime import datetime
from dati_fiumi import Rivers, MYSQLRivers

########################################

def on_message_callback(client, userdata, message:mqtt.MQTTMessage):

    if message.payload.decode() == '3 file creati! è ora di salvarli':
        print(message.payload.decode())
        manager.save(debug=False)#new_observation=True) #debug = True
    elif message.payload.decode() == 'Dati storici in arrivo! è ora di salvarli':
        print(message.payload.decode())
        manager.save(debug=False)
    #elif message.payload.decode() == 'Nuove osservazioni salvate, cosa ha in serbo il futuro per noi?':
        #print(message.payload.decode())
        #manager.make_predictions()
    elif message.payload.decode() == 'Previsioni completate, salvale!':
        print(message.payload.decode())
        manager.save(prediction=True) #debug = True
    
########################################
    
manager = MYSQLRivers()
broker_address= "broker.emqx.io"   
client = mqtt.Client('fiumi-storer') 
client.connect(broker_address, port = 1883, keepalive = 60) 
client.subscribe(os.environ.get('topic')) 
client.on_message = on_message_callback
client.loop_forever()
