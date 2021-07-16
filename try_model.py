import pickle
import streamlit as st
import numpy as np
import mysql
import os
import matplotlib.pyplot as plt
import pandas as pd 
import mysql.connector
import pandas as pd 
import numpy as np 
import os 
import pickle
import datetime
from pandas import *
from pandas.plotting import lag_plot, autocorrelation_plot
import matplotlib.pyplot as plt 
from statsmodels.tsa.ar_model import AutoReg
from statsmodels.graphics.tsaplots import plot_acf
from sklearn.metrics import mean_squared_error
from math import sqrt
import mysql.connector
from statsmodels.graphics.tsaplots import plot_pacf
from statsmodels.tsa.arima_process import ArmaProcess
from statsmodels.tsa.stattools import pacf
from statsmodels.regression.linear_model import yule_walker
from statsmodels.tsa.stattools import adfuller
from mysql.connector import connection
import seaborn as sns 
from PIL import Image
from streamlit_folium import folium_static
import folium
import json
from bokeh.plotting import figure
from Analysis_FINAL import * 
import pymysql

connection = mysql.connector.connect(
        host=  os.environ.get('host'), 
        port=  3310,
        database = 'database_fiumi',  #'rivers_db'
        user =  os.environ.get('user'),
        password = os.environ.get('password')
        )
connection.autocommit = True
cursor = connection.cursor()

def query_db(data_set_name,variable_name_key):
    query = 'SELECT Timestamp, {variable} from Tabella_{name}'.format(variable = variable_name_key , name = data_set_name)

    df = pd.read_sql(query, con=connection)
    # CLEAN THE DATA
    droppare = []
    i = 0 
    while i < (len(df[variable_name_key])):
        if np.isnan(df[variable_name_key].iloc[i]):
                droppare.append(i)
        i += 1

    for j in droppare :
        df =  df.drop(j)

    df.set_index('Timestamp')
    
    return df

df = query_db('Adige', 'W_mean')


path = 'E:/' # We saved the models into a USB pen 
model_name = 'Tabella_Adige-W_mean_model'
filename = path  + model_name
results = pickle.load(open(filename, 'rb'))
start_pred = len(df)
pred_time = start_pred + 5 
pred = results.get_prediction(start = pred_time , dynamic=False)
pred_ci = pred.conf_int()