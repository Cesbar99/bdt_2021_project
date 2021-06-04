#PUBLISHER
import time
import paho.mqtt.client as mqtt 

'''
client = mqtt.Client(client_id = 'station-storer') #create a new client
#client.usarename_ow_set('bdt-2021', '') #the broaker is protected by an authentication 
client.connect('broker.emqx.io') #define the host. 
while True:
    client.publish('bdt-2021/test', 'this is a test') # I can decide the Qos and if retain or not the message.

    time.sleep(5)
'''
'''
client = mqtt.Client(client_id = 'fiumi-sender') #create a new client
#client.usarename_ow_set('bdt-2021', '') #the broaker is protected by an authentication 
client.connect("mqtt.eclipseprojects.io") #define the host. 
while True:
    dic = {'casa':'Trento', 'Albergo': 'Napoli'}
    client.publish('fiumi', str(dic)) # I can decide the Qos and if retain or not the message.
    time.sleep(5)
'''
def publisher_dic(dic:dict):
    client = mqtt.Client(client_id = 'fiumi-sender') #create a new client
    #client.usarename_ow_set('bdt-2021', '') #the broaker is protected by an authentication 
    client.connect("broker.emqx.io", 1883, 60) #define the host. #alternatively "broker.emqx.io" "mqtt.eclipse.org"
    client.publish('fiumi', str(dic)) # I can decide the Qos and if retain or not the message.
    
def publisher_str(stringa:str):
    client = mqtt.Client(client_id = 'fiumi-sender') #create a new client
    #client.usarename_ow_set('bdt-2021', '') #the broaker is protected by an authentication 
    client.connect("broker.emqx.io") #define the host. #alternatively "broker.emqx.io" "mqtt.eclipse.org"
    client.publish('fiumi', stringa) # I can decide the Qos and if retain or not the message.