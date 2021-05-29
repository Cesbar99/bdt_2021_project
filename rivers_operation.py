from __future__ import absolute_import, annotations
from apscheduler.schedulers.blocking import BlockingScheduler

import textwrap
import os
import json
import time
from datetime import datetime
from typing import List, Optional
import requests
#import pyodbc

from dati_fiumi import MYSQLRivers, manager_dati_nuovi
from dati_fiumi import Manager_dati_storici
from mqtt_fiumi_publisher import publisher


#os.chdir('C:/Users/Cesare/OneDrive/studio/magistrale-data science/big data tech')

manager_mysql = MYSQLRivers()
manager_mysql.create()

url = 'http://dati.retecivica.bz.it/services/meteo/v1/sensors'
manager = manager_dati_nuovi()
manager.manage_new_rivers(url)
manager.publish_new_rivers()

'''
scheduler = BlockingScheduler()
scheduler.add_job(to_run_hourly, 'interval', seconds=10)
scheduler.start()
'''
