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



df = df.reset_index()
y_19 = df.iloc[:7107]
y_20 = df.iloc[7107:15888]
y_21 = df.iloc[15922:]



plt.plot(y_21['WT_mean'], label = '2021')
plt.plot(y_20['WT_mean'], color = 'green', label= '2020')
plt.plot(y_19['WT_mean'], color = 'red', label = '2019' )
plt.xlabel('Date')
plt.ylabel('Water Level')
plt.title('Water level Isarco')
plt.legend()

st.line_chart(df['WT_mean'])