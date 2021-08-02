import os
import paho.mqtt.client as mqtt 

def publisher_str(stringa:str):
    client = mqtt.Client(client_id = 'fiumi-sender') 
    client.connect("broker.emqx.io" ) 
    return client.publish(topic = os.environ.get('topic'), payload = stringa) 