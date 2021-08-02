from __future__ import absolute_import, annotations

import textwrap
import os
import json
import pandas as pd
from dati_fiumi import Rivers
from dati_fiumi import Manager_dati_storici
from mqtt_fiumi_publisher import publisher_str


with open('historic_data.json', 'r') as f: #latest.json
    file_reader = json.load(f) 
    to_insert = []
    to_insert_talvera = []
    to_insert_isarco = []
    to_insert_adige = []
    for diz in file_reader:

        manager = Manager_dati_storici(diz)
        manager.manage_historic_river()

        if diz["NAME"] == "TALFER BEI BOZEN/TALVERA A BOLZANO":
            to_insert_talvera.append(Rivers.to_repr(Rivers.from_repr(diz)))
        elif diz["NAME"] == "EISACK BEI BOZEN S\u00c3\u0153D/ISARCO A BOLZANO SUD":
            to_insert_isarco.append(Rivers.to_repr(Rivers.from_repr(diz)))
        elif diz["NAME"] == "ETSCH BEI SIGMUNDSKRON/ADIGE A PONTE ADIGE":
            to_insert_adige.append(Rivers.to_repr(Rivers.from_repr(diz)))

        #to_insert.append(diz)
        print(diz)


with open("created_json_talvera.json", "w") as target1:
    json.dump(to_insert_talvera, target1, default=str, ensure_ascii=False)

with open("created_json_isarco.json", "w") as target2:
    json.dump(to_insert_isarco, target2, default=str, ensure_ascii=False)

with open("created_json_adige.json", "w") as target3:
    json.dump(to_insert_adige, target3, default=str, ensure_ascii=False)


df = pd.read_json('created_json_talvera.json')
del df['NAME']
export_csv = df.to_csv('created_csv_talvera.csv', index = None, header=True)

df = pd.read_json('created_json_isarco.json')
del df['NAME']
export_csv = df.to_csv('created_csv_isarco.csv', index = None, header=True)

df = pd.read_json('created_json_adige.json')
del df['NAME']
export_csv = df.to_csv('created_csv_adige.csv', index = None, header=True)

publisher_str('3 file creati! è ora di salvarli')
