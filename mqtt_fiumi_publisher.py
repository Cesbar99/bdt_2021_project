import os
import time
import paho.mqtt.client as mqtt 

def publisher_str(stringa:str):
    client = mqtt.Client(client_id = 'fiumi-sender') 
    client.connect("broker.emqx.io") 
    client.publish(os.environ.get('topic'), stringa) 