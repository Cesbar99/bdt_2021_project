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


connection = mysql.connector.connect(
        host=  os.environ.get('host'), 
        port=  3310,
        database = 'database_fiumi',  #'rivers_db'
        user =  os.environ.get('user'),
        password = os.environ.get('password')
        )
connection.autocommit = True
cursor = connection.cursor()
query = 'SELECT Timestamp, WT_mean from Tabella_Talvera' 
df = pd.read_sql(query, con=connection)
# print(df)

df = pd.read_sql(query, con=connection)

# CLEAN THE DATA
droppare = []
i = 0 
while i < (len(df['WT_mean'])):
        if np.isnan(df['WT_mean'].iloc[i]):
                droppare.append(i)
        i += 1

for j in droppare :
    df =  df.drop(j)

st.title('Analysis of Rivers in the Bolzano\'s Area')

audio_file = open('river.ogg', 'rb')
audio_bytes = audio_file.read()
st.audio(audio_bytes, format='audio/ogg')

# Create map object
m = folium.Map(location=[46.486835, 11.335177], zoom_start=12) # COORDINATE BOZEN 

# Global tooltip
tooltip = 'Click For More Info'
# Create custom marker icon
# logoIcon = folium.features.CustomIcon('logo.png', icon_size=(50, 50))

# Vega data
#vis = os.path.join('data', 'vis.json')

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


# CHECKBOX TO SHOW DATAFRAMES 

if st.checkbox('Show dataframe'):
    chart_data = df

    chart_data


# WIDGET 
st.sidebar.header('Menù')

add_selectbox = st.sidebar.selectbox(
    "which river you want to know about ?",
    ("Adige", "Isarco", "Talvera")
)
