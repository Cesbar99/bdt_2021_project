#TO RUN ON PROMPT EXECUTE THE FOLLOWING CODE WITHOUT IMPORTING TIME AND FUNCTION TIME.SLEEP

#########
import paho.mqtt.client as mqtt #import the client1
import time
from datetime import datetime

from dati_fiumi import Rivers, MYSQLRivers

############
'''
def on_message(client, userdata, message):
    #print("message received " ,str(message.payload.decode("utf-8")))
    print('message received')
    print(message.payload.decode())
    dic = eval(message.payload.decode())
    print(dic['ID'], dic['NAME'])
    #print(dict(message.payload.decode())['Albergo'])
########################################
#broker_address="broker.hivemq.com"
broker_address="mqtt.eclipseprojects.io" #"iot.eclipse.org"
client = mqtt.Client('fiumi-storer') #client = mqtt.Client() create new instance ; client = mqtt.Client()
client.on_message=on_message #attach function to callback
client.connect(broker_address) #connect to broker
#client.loop_start() #start the loop
client.subscribe('fiumi') #client.subscribe('testtopic/#')
#client.loop_stop()
client.loop_forever()
#time.sleep(10) # wait
'''
def on_message(client, userdata, message):
    #print("message received " ,str(message.payload.decode("utf-8")))
    #print('message received')
    #print(message.payload.decode())
    #manager = MYSQLRivers()
    dic = eval(message.payload.decode())
    print(dic)
    #river = Rivers.from_repr(dic)
    #print(Rivers.to_repr(river))
    #manager.save(river)
    
########################################

#broker_address="broker.hivemq.com"
broker_address= "broker.emqx.io" #"iot.eclipse.org"
client = mqtt.Client('fiumi-storer') #client = mqtt.Client() create new instance ; client = mqtt.Client()
client.connect(broker_address) #connect to broker
client.subscribe('fiumi') #client.subscribe('testtopic/#')
client.on_message=on_message #attach function to callback
client.loop_forever()
#client.loop_start() #start the loop
#client.loop_stop()

#time.sleep(10) # wait


