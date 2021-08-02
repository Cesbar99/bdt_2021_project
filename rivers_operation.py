import time
from dati_fiumi import MYSQLRivers, manager_dati_nuovi
from mqtt_fiumi_publisher import publisher_str
import paho.mqtt.client as mqtt

manager_mysql = MYSQLRivers()
manager_mysql.create()
manager_mysql.connection.close()
while True:

    url = 'http://dati.retecivica.bz.it/services/meteo/v1/sensors'
    manager = manager_dati_nuovi()
    manager.manage_new_rivers(url)
    manager.transfer_json()
    manager.from_json_to_csv()
    publisher_str('3 file creati! è ora di salvarli')
    print('finito')
    time.sleep(3600) #3600




