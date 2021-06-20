import pandas as pd 
import numpy as np 
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


connection = mysql.connector.connect(
        host= 'ec2-18-117-169-228.us-east-2.compute.amazonaws.com', #'127.0.0.1'
        port=  3310,
        database = 'database_fiumi',  #'rivers_db'
        user = 'root',
        password = 'password'
        )
connection.autocommit = True
cursor = connection.cursor()

# GET THE DATA

query = 'SELECT Timestamp, W_mean from Try_Isarco' 
df = pd.read_sql(query, con=connection)


# CLEAN THE DATA
droppare = []
i = 0 
while i < (len(df['W_mean'])):
        if np.isnan(df['W_mean'][i]):
                droppare.append(i)		
        i += 1

for j in droppare :
    df =  df.drop(j)

# START THE ANALYSIS	
df.plot(x = 'Timestamp', y = 'W_mean')
plt.title('Water level - time series')
plt.xlabel('date')
plt.ylabel('Water level (cm)')
#plt.show()

# CHECK FOR AUTOCORRELATION  
values = DataFrame(df['W_mean'].values)
dataframe = concat([values.shift(1), values], axis=1)
dataframe.columns = ['t-1', 't+1']
result = dataframe.corr()
# AUTOCORRELATION PLOT
autocorrelation_plot(df['W_mean'])
plot_acf(df['W_mean'], lags=31)
#plt.show()
# split into train and test sets
X = df['W_mean'].values
# split dataset
t_più_uno = 7 
train, test = X[1:len(X)-t_più_uno], X[len(X)-t_più_uno:]


# train autoregression
window = 365
model = AutoReg(train, lags=window)
model_fit = model.fit()
coef = model_fit.params

# walk forward over time steps in test
history = train[len(train)-window:]
history = [history[i] for i in range(len(history))]
predictions = list()
for t in range(len(test)):
    length = len(history)
    lag = [history[i] for i in range(length-window,length)]
    yhat = coef[0]
    for d in range(window):
        yhat += coef[d+1] * lag[window-d-1]
    obs = test[t]
    predictions.append(yhat)
    history.append(obs)
    print('predicted=%f, expected=%f' % (yhat, obs))
rmse = sqrt(mean_squared_error(test, predictions))
print('Test RMSE: %.3f' % rmse)




'''# plot
plt.plot(test)
plt.plot(predictions, color='red')
plt.title('Autoregressive Model')
plt.show()'''

plt.figure(figsize=[15, 7.5]) # Set dimensions for figure
plt.scatter(train, train)
plt.title('')
plt.ylabel('')
plt.xlabel('')
plt.xticks(rotation=90)
plt.grid(True)
#plt.show()

plt.figure(figsize=[15, 7.5]); # Set dimensions for figure
plt.plot(train) #df['W_mean]
plt.title("")
#plt.show()

ad_fuller_result = adfuller(train)
print(f'ADF Statistic: {ad_fuller_result[0]}')
print(f'p-value: {ad_fuller_result[1]}') # it is stationary
plot_pacf(train)
plot_acf(train) 
plt.show()

# Try a AR(4) model
var = 2 # Lowest rmse 
rho, sigma = yule_walker(df['W_mean'], var)
print(f'rho: {-rho}')
print(f'sigma: {sigma}')

# PREDICTION 
# 1hr
predictions = []

for i in range(1, t_più_uno + 1):
    print(-var-t_più_uno +i -2 , -t_più_uno + i -2)
    x_prev = train[-var-t_più_uno +i -2 : -t_più_uno + i -2 ]
    pred = sum(x_prev * rho) + sigma
    predictions.append(pred)


# Results  
print('predictions:', predictions, 'actual:', test)
# ACCURACY 
#rmse = sqrt(mean_squared_error(test, predictions))
#print(rmse)


cursor.close() 


