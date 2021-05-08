import requests
import json

url = "https://os.smartcommunitylab.it/core.mobility/bikesharing/trento"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

#print(response.text)

#bike_tn = json.loads(response.text)

#print(bike_tn)

with open('C:/Users/Cesare/OneDrive/Desktop/bike_tn', 'r') as f:
    #json.dump(data, f)
    data = json.load(f)

    for stazione in data:
        print(stazione['name']) 


'''
import json 
import requests

url = "https://os.smartcommunitylab.it/core.mobility/bikesharing/trento"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data =payload)

#print(response.text)
#print(response.json())

bike_tn = json.loads(response.text)
counter = 0 
for data in bike_tn: 
    counter += data['totalSlots']
    #print(data['name'])

print(counter)'''












