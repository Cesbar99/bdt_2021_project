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
    ("Water Level", "Water Temperature", "Flow_Rate")
)

diz_measures = {'Water Level' : 'W_mean', 'Water Temperature': 'WT_mean', 'Flow_Rate' : 'Q_mean'}

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
st.line_chart(query_db(data_set_name,variable_name_key)[diz_measures[variable_name_key]])

# CHECKBOX FOR PREDICTION 

#st.selectbox('Choose the prediction time',
#('two hour','Three hours', 'Twelve hours', 'two day', 'Three Days' 'two week'))

time = st.slider('Decide how far to move in the future (hrs) ', 1 , 168)

diz_times = {1: 'two hour',
            2: 'two hours',
            3: 'three hours',
            4 : 'four hours',
            5 : 'five hours', 
            6 : 'six hours', 
            7 : 'seven hours',
            8 : 'eight hours',
            9 : 'nine hours', 
            10 : 'ten hours',
            11: 'eleven hours',
            12 : 'twelve hours', 
            13 : 'thirteen hours',
            14 : 'fourteen hours',
            15 : 'fifteen hours', 
            16 : 'sixteen hours', 
            17 : 'seventeen hours', 
            18 : 'eighteen hours', 
            19 : 'nineteen hours',
            20 : 'twenty hours', 
            21 : 'twenty two hours',
            22 : 'twenty two hours', 
            23 : 'tenty three hours', 
            24 : 'one day',
            25 :  'one day & one hour',
            26 :  'one day & two hours',
            27 :  'one day & three hours',
            28 : 'one day & four hours',
            29 : 'one day & five hours', 
            30 : 'one day & six hours', 
            31 : 'one day & seven hours',
            32 : 'one day & eight hours',
            33 : 'one day & nine hours', 
            34 : 'one day & ten hours',
            35:  'one day & eleven hours',
            36 : 'one day & twelve hours', 
            37 : 'one day & thirteen hours',
            38 : 'one day & fourteen hours',
            39 : 'one day & fifteen hours', 
            40 : 'one day & sixteen hours', 
            41 : 'one day & seventeen hours', 
            42 : 'one day & eighteen hours', 
            43 : 'one day & nineteen hours',
            44 : 'one day & twenty hours', 
            45 : 'one day & twenty one hours',
            46 : 'one day & twenty two hours', 
            47 : 'one day & tenty three hours', 
            48 : 'two days',
            49 :  'two days & one hour',
            50 :  'two days & two hours',
            51 :  'two days & three hours',
            52 : 'two days & four hours',
            53 : 'two days & five hours', 
            54 : 'two days & six hours', 
            55 : 'two days & seven hours',
            56 : 'two days & eight hours',
            57 : 'two days & nine hours', 
            58 : 'two days & ten hours',
            59:  'two days & eleven hours',
            60 : 'two days & twelve hours', 
            61 : 'two days & thirteen hours',
            62 : 'two days & fourteen hours',
            63 : 'two days & fifteen hours', 
            64 : 'two days & sixteen hours', 
            65 : 'two days & seventeen hours', 
            66 : 'two days & eighteen hours', 
            67 : 'two days & nineteen hours',
            68 : 'two days & twenty hours', 
            69 : 'two days & twenty two hours',
            70 : 'two days & twenty two hours', 
            71 : 'two days & tenty three hours', 
            72 : 'three days',
            73 :  'three days & one hour',
            74 :  'three days & two hours',
            75 :  'three days & three hours',
            76 : 'three days & four hours',
            77 : 'three days & five hours', 
            78 : 'three days & six hours', 
            79 : 'three days & seven hours',
            80 : 'three days & eight hours',
            81 : 'three days & nine hours', 
            82 : 'three days & ten hours',
            83:  'three days & eleven hours',
            84 : 'three days & twelve hours', 
            85 : 'three days & thirteen hours',
            86 : 'three days & fourteen hours',
            87 : 'three days & fifteen hours', 
            88 : 'three days & sixteen hours', 
            89 : 'three days & seventeen hours', 
            90 : 'three days & eighteen hours', 
            91 : 'three days & nineteen hours',
            92 : 'three days & twenty hours', 
            93 : 'three days & twenty one hours',
            94 : 'three days & twenty two hours', 
            95 : 'three days & tenty three hours', 
            96 : 'four days',
            97 :  'four days & one hour',
            98 :  'four days & two hours',
            99 :  'four days & three hours',
            100 : 'four days & four hours',
            101 : 'four days & five hours', 
            102 : 'four days & six hours', 
            103 : 'four days & seven hours',
            104 : 'four days & eight hours',
            105 : 'four days & nine hours', 
            106 : 'four days & ten hours',
            107:  'four days & eleven hours',
            108 : 'four days & twelve hours', 
            109 : 'four days & thirteen hours',
            110 : 'four days & fourteen hours',
            111 : 'four days & fifteen hours', 
            112 : 'four days & sixteen hours', 
            113 : 'four days & seventeen hours', 
            114 : 'four days & eighteen hours', 
            115 : 'four days & nineteen hours',
            116 : 'four days & twenty hours', 
            117: 'four days & twenty one hours',
            118: 'four days & twenty two hours', 
            119: 'four days & tenty three hours', 
            120: 'five days',
            121 :  'five days & one hour',
            122:  'five days & two hours',
            123:  'five days & three hours',
            124 : 'five days & four hours',
            125 : 'five days & five hours', 
            126 : 'five days & six hours', 
            127 : 'five days & seven hours',
            128 : 'five days & eight hours',
            129 : 'five days & nine hours', 
            130 : 'five days & ten hours',
            131:  'five days & eleven hours',
            132 : 'five days & twelve hours', 
            133 : 'five days & thirteen hours',
            134 : 'five days & fourteen hours',
            135 : 'five days & fifteen hours', 
            136 : 'five days & sixteen hours', 
            137 : 'five days & seventeen hours', 
            138 : 'five days & eighteen hours', 
            139 : 'five days & nineteen hours',
            140 : 'five days & twenty hours', 
            141: 'five days & twenty one hours',
            142: 'five days & twenty two hours', 
            143: 'five days & tenty three hours', 
            144: 'six days',
            145 :  'six days & one hour',
            146:  'six days & two hours',
            147 :  'six days & three hours',
            148 : 'six days & four hours',
            149 : 'six days & five hours', 
            150 : 'six days & six hours', 
            151 : 'six days & seven hours',
            152 : 'six days & eight hours',
            153 : 'six days & nine hours', 
            154 : 'six days & ten hours',
            155:  'six days & eleven hours',
            156 : 'six days & twelve hours', 
            157 : 'six days & thirteen hours',
            158 : 'six days & fourteen hours',
            159 : 'six days & fifteen hours', 
            160 : 'six days & sixteen hours', 
            161 : 'six days & seventeen hours', 
            162 : 'six days & eighteen hours', 
            163 : 'six days & nineteen hours',
            164 : 'six days & twenty hours', 
            165: 'six days & twenty one hours',
            166: 'six days & twenty two hours', 
            167: 'six days & tenty three hours', 
            168: 'one week '}


#st.write('The {variable} in {time_correct} will be {result}'.format(variable = variable_name_key, time_correct = diz_times[time], result = prediction_interval(time, variable_name_key)  ))
