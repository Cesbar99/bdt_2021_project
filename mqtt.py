import os
from paho.mqtt import client as mqtt
from dati_fiumi import MYSQLRivers

########################################

def on_message_callback(client, userdata, message:mqtt.MQTTMessage):

    if message.payload.decode() == '3 file creati! è ora di salvarli':
        manager = MYSQLRivers()
        print(message.payload.decode())
        manager.save(debug=False) #new_observation=True) #debug = True
        manager.connection.close()
    elif message.payload.decode() == 'Dati storici in arrivo! è ora di salvarli':
        manager = MYSQLRivers()
        print(message.payload.decode())
        manager.save(debug=False)
        manager.connection.close()
    elif message.payload.decode() == 'Previsioni completate, salvale!':
        manager = MYSQLRivers()
        print(message.payload.decode())
        manager.save(prediction=True) #debug = True
        manager.connection.close()
    
########################################
    

broker_address= "broker.emqx.io"   
client = mqtt.Client('fiumi-storer') 
client.connect(broker_address, port = 1883, keepalive = 60) 
client.subscribe(os.environ.get('topic')) 
client.on_message = on_message_callback
client.loop_forever()
