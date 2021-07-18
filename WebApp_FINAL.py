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
import joblib

connection = mysql.connector.connect(
        host=  os.environ.get('host'), 
        port=  3310,
        database = 'database_fiumi',  #'rivers_db'
        user =  os.environ.get('user'),
        password = os.environ.get('password')
        )
connection.autocommit = True
cursor = connection.cursor()





st.title('Analysis of Rivers in the Bolzano\'s Area')

audio_file = open('river.ogg', 'rb')
audio_bytes = audio_file.read()
st.audio(audio_bytes, format='audio/ogg')

# Create map object
m = folium.Map(location=[46.486835, 11.335177], zoom_start=12) # COORDINATE BOZEN 
# Global tooltip
tooltip = 'Click For More Info'
# Percorso talvera 
path_t = [(46.495619, 11.347877), (46.499680, 11.346658), (46.506887, 11.349619), (46.510753, 11.350403), (46.511931, 11.350571), (46.513579, 11.353640), (46.514927,11.357624),
(46.517354, 11.357581)]
path_pa = [(46.487316, 11.290921),(46.482711, 11.304865),(46.479067, 11.307707), (46.474282, 11.309064), (
46.467408, 11.303634), (46.458519, 11.301413), (46.445858, 11.309212)]
path_is = [(46.494716, 11.387254), (46.492876, 11.370051), (46.493996, 11.353545),(46.486874, 11.334018),(46.476998, 11.315410),(46.465194, 11.304854),
(46.450954, 11.306785)]
# Geojson Data
overlay = os.path.join('data', 'overlay.json')
# Create markers
folium.Marker([46.494716, 11.387254],
                popup='<strong>Isarco</strong>',
                tooltip=tooltip).add_to(m),
folium.Marker([46.517354, 11.357581],
                popup='<strong>Talvera</strong>',
                tooltip=tooltip).add_to(m),
folium.Marker([46.487316, 11.290921],
                popup='<strong> Ponte Adige </strong>',
                tooltip=tooltip).add_to(m),
folium.PolyLine(path_t,
                    color='steelblue',
                    weight= 5,
                    opacity=0.8).add_to(m),
folium.PolyLine(path_pa,
                    color='dodgerblue',
                    weight= 5,
                    opacity=0.8).add_to(m),
folium.PolyLine(path_is,
                    color='cadetblue',
                    weight= 5,
                    opacity=0.8).add_to(m),

# call to render Folium map in Streamlit
folium_static(m)

# SIDE BARS

# WIDGET 
st.sidebar.header('Menù')

data_set_name =  add_selectbox = st.sidebar.selectbox(
    "which river do you want to know about ?",
    ("Adige", "Isarco", "Talvera")
)

variable_name_key =  add_selectbox = st.sidebar.selectbox(
    "which measure do you want to know about ?",
    ("Water Level", "Water Temperature", "Water Flow")
)

diz_measures = {'Water Level' : 'W_mean', 'Water Temperature': 'WT_mean', 'Water Flow' : 'Q_mean'}
diz_unit = {'Water Level' : 'Water Level (cm)', 'Water Temperature': 'Water Temperature (°C)', 'Water Flow' : 'Water Flow (m³/s)'}


def query_db(data_set_name,variable_name_key):
    query = 'SELECT Timestamp, {variable} from Tabella_{name}'.format(variable = diz_measures[variable_name_key] , name = data_set_name)

    df = pd.read_sql(query, con=connection)
    # CLEAN THE DATA
    droppare = []
    i = 0 
    while i < (len(df[diz_measures[variable_name_key]])):
        if np.isnan(df[diz_measures[variable_name_key]].iloc[i]):
                droppare.append(i)
        i += 1

    for j in droppare :
        df =  df.drop(j)

    df.set_index('Timestamp')
    
    return df

df = query_db(data_set_name, variable_name_key)


# CHECKBOX TO SHOW DATAFRAMES 
if st.checkbox('Show dataframe'):
    chart_data = query_db(data_set_name,variable_name_key)

    chart_data

st.write('Let\'s take a look to the data')
df2 = df
st.line_chart(df2.rename(columns={'Timestamp':'index'}).set_index('index'))
#st.line_chart(df[diz_measures[variable_name_key]])

# CHECKBOX FOR PREDICTION 

time = st.selectbox('Choose the prediction time',
('One hour','Three hours', 'Twelve hours', 'One day', 'Three Days','One week'))

<<<<<<< Updated upstream
diz_time = {'One hour': 1, 'Three hours' :2 , 'Twelve hours': 3, 'One day' : 4, 'Three Days' : 5,'One week':6}


def analysis(data_set_name,variable_name_key, time):
    #path = 'E:/' # We saved the models into a USB pen 
    #model_name = 'Tabella_' + data_set_name + '-' + diz_measures[variable_name_key] + '_model'
    #filename = path  + model_name
    #results = joblib.load(open(filename, 'rb'))
    #start_pred = len(df)
    #pred_time = start_pred + diz_time[time] 
    #pred = results.get_prediction(start = pred_time , dynamic=False)
    #pred_ci = pred.conf_int()
    query = 'select * from pred_{name}_{variable} ORDER BY Timestamp DESC LIMIT 1;'.format(name =data_set_name, variable = diz_measures[variable_name_key])  #pred_Adige_Q_mean
    cursor.execute(query)
    output = cursor.fetchall()
    output = output[0]
    output = output[diz_time[time]]
    
    
=======
diz_time = {'One hour': 1, 'Three hours' :3 , 'Twelve hours': 12, 'One day' : 24, 'Three Days' : 72,'One week':168}


def analysis(data_set_name,variable_name_key, time):
    path = 'E:/' # We saved the models into a USB pen 
    model_name = 'Tabella_' + data_set_name + '-' + diz_measures[variable_name_key] + '_model'
    filename = path  + model_name
    results = joblib.load(open(filename, 'rb'))
    start_pred = len(df)
    pred_time = start_pred + diz_time[time] 
    pred = results.get_prediction(start = pred_time , dynamic=False)
    pred_ci = pred.conf_int()
    
    output_l = str(pred_ci['lower variable_actual']).split()
    output_u = str(pred_ci['upper variable_actual']).split()
    output = output_l[1] + ' - ' + output_u[1]
    plt.figure(figsize=(16,10), dpi=100)
    ax = df[diz_measures[variable_name_key]][:].plot(label= 'observed')
    
    pred.predicted_mean.plot(ax=ax, label='Forecast')
    ax.fill_between(pred_ci.index,
                    pred_ci.iloc[:, 0],
                    pred_ci.iloc[:, 1], color='grey', alpha=1, label = 'confidence interval')
    ax.set_xlabel('Date')
    ax.set_ylabel(diz_unit[variable_name_key])
    plt.legend()
    plt.xlim([start_pred -150,start_pred + 200])
    plt.title('Prediction of {river_name}\'s {variable} in {time_correct}  will be in between {result}'.format(river_name = data_set_name,variable = variable_name_key, time_correct = time, result = output))
    st.set_option('deprecation.showPyplotGlobalUse', False)
    st.pyplot()
>>>>>>> Stashed changes
    return output

prediction_interval = analysis(data_set_name,variable_name_key, time)
#st.write(prediction_interval)

st.write('{river_name}\'s {variable} in {time_correct}  will be in between {result}'.format(river_name = data_set_name,variable = variable_name_key, time_correct = time, result = prediction_interval))




    

    

    


    