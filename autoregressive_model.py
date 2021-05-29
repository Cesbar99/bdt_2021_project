import pandas as pd 
from pandas import *
from pandas.plotting import lag_plot, autocorrelation_plot
import matplotlib.pyplot as plt 
from statsmodels.tsa.ar_model import AutoReg
from sklearn.metrics import mean_squared_error
from math import sqrt
import mysql.connector
from mysql.connector import connection


connection = mysql.connector.connect(
        host= '127.0.0.1',
        port=  3310,
        database = 'rivers_db',
        user = 'root',
        password = 'password'
        )
connection.autocommit = True
cursor = connection.cursor()

# GET THE DATA
query = 'SELECT Timestamp, W_mean from Tabella_Isarco'
cursor.execute(query)
series = cursor.fetchall() # series should represente the timestamp and the w_mean for all the observations
series.plot() #  line plot of the dataset is then created
plt.show()

# CHECK FOR AUTOCORRELATION  
lag_plot(series)
plt.show()
#EXPECTED OUTPUT : We should see a large ball of observations along a diagonal line of the plot.
#  It clearly shows a relationship or some correlation.
values = DataFrame(series.values)
dataframe = concat([values.shift(1), values], axis=1)
dataframe.columns = ['t-1', 't+1']
result = dataframe.corr()
print(result)
# AUTOCORRELATION PLOT
autocorrelation_plot(series)
plt.show()
# PERSISTENCE MODEL 
'''The simplest model that we could use to make predictions would be to persist the last observation. 
We can call this a persistence model and it provides a baseline of performance for the problem 
that we can use for comparison with an autoregression model.

We can develop a test harness for the problem by splitting the observations into training and test sets,
with only the last 7 observations in the dataset assigned to the test set as “unseen” data 
that we wish to predict.

The predictions are made using a walk-forward validation model so
that we can persist the most recent observations for the next day. 
This means that we are not making a 7-day forecast, but 7 1-day forecasts.
'''
# create lagged dataset
values = DataFrame(series.values)
dataframe = concat([values.shift(1), values], axis=1)
dataframe.columns = ['t-1', 't+1']
# split into train and test sets
X = dataframe.values
train, test = X[1:len(X)-7], X[len(X)-7:] # We need to modify this because we do not have daily information
train_X, train_y = train[:,0], train[:,1]
test_X, test_y = test[:,0], test[:,1]
 
# persistence model
def model_persistence(x):
	return x
 
# walk-forward validation
predictions = list()
for x in test_X:
	yhat = model_persistence(x)
	predictions.append(yhat)
test_score = mean_squared_error(test_y, predictions)
print('Test MSE: %.3f' % test_score)
# plot predictions vs expected
plt.plot(test_y)
plt.plot(predictions, color='red')
plt.show()

# AUTOREGRESSIVE MODEL 
# split dataset
X = series.values
train, test = X[1:len(X)-7], X[len(X)-7:]
# train autoregression
model = AutoReg(train, lags=29)
model_fit = model.fit()
print('Coefficients: %s' % model_fit.params)
# make predictions
predictions = model_fit.predict(start=len(train), end=len(train)+len(test)-1, dynamic=False)
for i in range(len(predictions)):
	print('predicted=%f, expected=%f' % (predictions[i], test[i]))
rmse = sqrt(mean_squared_error(test, predictions))
print('Test RMSE: %.3f' % rmse)
# plot results
plt.plot(test)
plt.plot(predictions, color='red')
plt.show()

'''The statsmodels API does not make it easy to update the model as new observations become available.

One way would be to re-train the AutoReg model each day as new observations become available, 
and that may be a valid approach, if not computationally expensive.

An alternative would be to use the learned coefficients and manually make predictions. 
This requires that the history of 29 prior observations be kept and that the coefficients be retrieved 
from the model and used in the regression equation to come up with new forecasts.

The coefficients are provided in an array with the intercept term followed by the coefficients
 for each lag variable starting at t-1 to t-n. We simply need to use them in the right order on the history 
 of observations, as follows:
'''
# split dataset
X = series.values
train, test = X[1:len(X)-7], X[len(X)-7:]
# train autoregression
window = 29
model = AutoReg(train, lags=29)
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
# plot
plt.plot(test)
plt.plot(predictions, color='red')
plt.show()


cursor.close()