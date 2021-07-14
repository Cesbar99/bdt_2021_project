import pandas as pd 
import numpy as np 
import os 
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

# CONNECTION
connection = mysql.connector.connect(
        host=  os.environ.get('host'), 
        port=  3310,
        database = 'database_fiumi',  #'rivers_db'
        user =  os.environ.get('user'),
        password = os.environ.get('password')
        )
connection.autocommit = True
cursor = connection.cursor()
# QUERY DATA BASE
query = 'SELECT Timestamp, W_mean from Tabella_Isarco' 
df = pd.read_sql(query, con=connection)
print(df)
# CLEAN THE DATA
droppare = []
i = 0 
while i < (len(df['W_mean'])):
        if np.isnan(df['W_mean'].iloc[i]):
                droppare.append(i)
        i += 1

for j in droppare :
    df =  df.drop(j)

df = df.reset_index()

df.describe()

#Create a df for the year 2019
y_19 = df.iloc[:7108]  ### ??? check 

#Create a df for the year 2020
y_20 = df.iloc[7108:16147]

#Create a df for the year 2021
y_21 = df.iloc[16147:]

#tks = np.arange(min(df['Timestamp']), max(df['Timestamp']), 30)
min_time = (min(df['Timestamp']))   
max_time = (max(df['Timestamp']))
ticks = []
#tks = np.arange(min_time, max_time)
start_time = df['Timestamp'].iloc[0]
start_time = str(start_time)
start_time = start_time.split()
start_time = start_time[0]
print(start_time)
x_data = pd.date_range(start_time, periods=30, freq='MS') 
# Check how this dates looks like:
print(x_data)
for i in range(len(df)):
    ticks.append(i)
    
len(ticks)


plt.figure(figsize=(16,10), dpi=100)
plt.plot(y_21['W_mean'], label = '2021')
plt.plot(y_20['W_mean'], color = 'green', label= '2020')
plt.plot(y_19['W_mean'], color = 'red', label = '2019' )
plt.xlabel('Date')
plt.ylabel('Water Level')
plt.title('Water level Isarco')
plt.legend()
plt.xticks()
# MODIFICARE I TICKS 
plt.show()

# Output the maximum and minimum temperature date
print(df.loc[df["W_mean"] == df["W_mean"].max()])
print(df.loc[df["W_mean"] == df["W_mean"].min()])

# Plot the daily temperature change 
plt.figure(figsize=(16,10), dpi=100)
plt.plot(df.index, df['W_mean'], color='tab:red')
# MODIFICARE I TICKS 
plt.gca().set(title="Water level Isarco", xlabel='Date', ylabel="Water Level")
plt.show()

from statsmodels.tsa.seasonal import seasonal_decompose

# Additive Decomposition
result_add = seasonal_decompose(df.W_mean, model='additive', extrapolate_trend='freq', freq=365)

# Plot
plt.rcParams.update({'figure.figsize': (10,10)})
result_add.plot().suptitle('Additive Decomposition', fontsize=22)
plt.show()

# Shift the current temperature to the next day. 
predicted_df = df["W_mean"].to_frame().shift(1).rename(columns = {"W_mean": "W_mean_pred" })
actual_df = df["W_mean"].to_frame().rename(columns = {"W_mean": "W_mean_actual" })

# Concatenate the actual and predicted temperature
one_step_df = pd.concat([actual_df,predicted_df],axis=1)

# Select from the second row, because there is no prediction for today due to shifting.
one_step_df = one_step_df[1:]
one_step_df.head(10)


from sklearn.metrics import mean_squared_error as MSE
from math import sqrt

# Calculate the RMSE
temp_pred_err = MSE(one_step_df.W_mean_actual, one_step_df.W_mean_pred, squared=False)
print("The RMSE is",temp_pred_err)


import itertools

# Define the p, d and q parameters to take any value between 0 and 2
p = d = q = range(0, 2)

# Generate all different combinations of p, q and q triplets
pdq = list(itertools.product(p, d, q))

# Generate all different combinations of seasonal p, q and q triplets
seasonal_pdq = [(x[0], x[1], x[2], 12) for x in list(itertools.product(p, d, q))]

print('Examples of parameter combinations for Seasonal ARIMA...')
print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[1]))
print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[2]))
print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[3]))
print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[4]))

import warnings
warnings.filterwarnings("ignore") # specify to ignore warning messages

for param in pdq:
    for param_seasonal in seasonal_pdq:
        try:
            mod = sm.tsa.statespace.SARIMAX(one_step_df.W_mean_actual,
                                            order=param,
                                            seasonal_order=param_seasonal,
                                            enforce_stationarity=False,
                                            enforce_invertibility=False)

            results = mod.fit()

            print('SARIMA{}x{}12 - AIC:{}'.format(param, param_seasonal, results.aic))
        except:
            continue

# Import the statsmodels library for using SARIMAX model
import statsmodels.api as sm

# Fit the SARIMAX model using optimal parameters
mod = sm.tsa.statespace.SARIMAX(one_step_df.W_mean_actual,
                                order=(1, 1, 1),
                                seasonal_order=(1, 1, 1, 12),
                                enforce_stationarity=False,
                                enforce_invertibility=False)

results = mod.fit()

results.plot_diagnostics(figsize=(15, 12))
plt.show()


start_pred = len(df)

pred_1h = start_pred +1
pred_3h = start_pred + 3
pred_12h = start_pred +12
pred_1d = start_pred + 24
pred_3d = start_pred + 72
pred_1w = start_pred + 168
pred_list = [pred_1h, pred_3h, pred_12h, pred_1d, pred_3d, pred_1w]
list_output = []

for pred_time in pred_list:
    pred = results.get_prediction(start = pred_time , dynamic=False)
    pred_ci = pred.conf_int()
    output_l = str(pred_ci['lower W_mean_actual']).split()
    output_u = str(pred_ci['upper W_mean_actual']).split()
    output = output_l[1] + ' - ' + output_u[1]
    list_output.append(output)
    plt.figure(figsize=(16,10), dpi=100)
    ax = one_step_df.W_mean_actual[:].plot(label='observed')
    pred.predicted_mean.plot(ax=ax, label='Forecast')

    ax.fill_between(pred_ci.index,
                    pred_ci.iloc[:, 0],
                    pred_ci.iloc[:, 1], color='grey', alpha=1, label = 'confidence interval')

    ax.set_xlabel('Date')
    ax.set_ylabel('Temperature (in Celsius)')
    plt.legend()
    plt.xlim([start_pred -100,pred_1w + 50])
    print(pred_ci.iloc[:, 0])
    print(pred_ci.iloc[:, 1])
    plt.show()