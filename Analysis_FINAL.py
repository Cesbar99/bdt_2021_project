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
import statsmodels.api as sm
from mysql.connector import connection
import seaborn as sns 
import pickle
from mqtt_fiumi_publisher import publisher_str
import joblib


def Analysis(river_name :str, variable :str):

    connection = mysql.connector.connect(
        host = os.environ.get('host'), #'ec2-18-117-169-228.us-east-2.compute.amazonaws.com', #'127.0.0.1'
        port =  3310,
        database = 'database_fiumi',  #'rivers_db'
        user = os.environ.get('user'), #root, user_new
        password = os.environ.get('password'), #password, passwordnew_user
        allow_local_infile = True
        )
    connection.autocommit = True
     
    # QUERY DATA BASE
    query = 'SELECT Timestamp, {variable} from {river_name}'.format(variable = variable,  river_name = river_name)
    df = pd.read_sql(query, con=connection)
    #print(df)
    # CLEAN THE DATA
    droppare = []
    i = 0 
    while i < (len(df[variable])):
            if np.isnan(df[variable].iloc[i]):
                    droppare.append(i)
            i += 1

    for j in droppare :
        df =  df.drop(j)

    df = df.reset_index()

    #df.describe()

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
    #print(start_time)
    x_data = pd.date_range(start_time, periods=30, freq='MS') 
    # Check how this dates looks like:
    #print(x_data)
    for i in range(len(df)):
        ticks.append(i)
        
    len(ticks)


    plt.figure(figsize=(16,10), dpi=100)
    plt.plot(y_21['Timestamp'], y_21[variable], label = '2021')
    plt.plot(y_20['Timestamp'], y_20[variable], color = 'green', label= '2020')
    plt.plot(y_19['Timestamp'],y_19[variable], color = 'red', label = '2019' )
    plt.xlabel('Date')
    name_river = river_name.split('_')
    name_river = name_river[1]
    if variable == 'W_mean':
        plt.ylabel('Water Level')
        plt.title('Water level {name}'.format(name = name_river))    
    elif variable == 'Q_mean':
        plt.ylabel('Flow rate')
        plt.title('Flow rate {name}'.format(name = name_river))
    else:
        plt.ylabel('Water Temperature')
        plt.title('Water Temperature {name}'.format(name = name_river))

    plt.legend()
    plt.xticks()
    # MODIFICARE I TICKS 
    #plt.show()

    # Output the maximum and minimum temperature date
    #print(df.loc[df[variable] == df[variable].max()])
    #print(df.loc[df[variable] == df[variable].min()])

    # Plot the daily temperature change 
    plt.figure(figsize=(16,10), dpi=100)
    plt.plot(df['Timestamp'], df[variable], color='tab:red')
    # MODIFICARE I TICKS 
    if variable == 'W_mean':
        plt.gca().set(title="Water level {name}".format(name = name_river) , xlabel='Date', ylabel="Water Level")    
    elif variable == 'Q_mean':
        plt.gca().set(title="Flow Rate {name}".format(name = name_river) , xlabel='Date', ylabel="Flow Rate")
    else:
        plt.gca().set(title="Water Temperature {name}".format(name = name_river) , xlabel='Date', ylabel="Water Temperature")
    
    #plt.show()

    from statsmodels.tsa.seasonal import seasonal_decompose

    # Additive Decomposition
    result_add = seasonal_decompose(df[variable], model='additive', extrapolate_trend='freq', freq=365)

    # Plot
    plt.rcParams.update({'figure.figsize': (10,10)})
    result_add.plot().suptitle('Additive Decomposition', fontsize=22)
    #plt.show()

    # Shift the current temperature to the next day. 
    predicted_df = df[variable].to_frame().shift(1).rename(columns = {variable: "variable_pred" })
    actual_df = df[variable].to_frame().rename(columns = {variable: "variable_actual" })

    # Concatenate the actual and predicted temperature
    one_step_df = pd.concat([actual_df,predicted_df],axis=1)

    # Select from the second row, because there is no prediction for today due to shifting.
    one_step_df = one_step_df[1:]
    #one_step_df.head(10)


    from sklearn.metrics import mean_squared_error as MSE
    from math import sqrt

    # Calculate the RMSE
    temp_pred_err = MSE(one_step_df.variable_actual, one_step_df.variable_pred, squared=False)
    #print("The RMSE is",temp_pred_err)


    import itertools

    # Define the p, d and q parameters to take any value between 0 and 2
    p = d = q = range(0, 2)

    # Generate all different combinations of p, q and q triplets
    pdq = list(itertools.product(p, d, q))

    # Generate all different combinations of seasonal p, q and q triplets
    seasonal_pdq = [(x[0], x[1], x[2], 12) for x in list(itertools.product(p, d, q))]

    '''
    print('Examples of parameter combinations for Seasonal ARIMA...')
    print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[1]))
    print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[2]))
    print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[3]))
    print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[4]))
    '''

    import warnings
    warnings.filterwarnings("ignore") # specify to ignore warning messages

    for param in pdq:
        for param_seasonal in seasonal_pdq:
            try:
                mod = sm.tsa.statespace.SARIMAX(df[variable], #one_step_df.variable_actual
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
    mod = sm.tsa.statespace.SARIMAX(one_step_df.variable_actual,
                                    order=(1, 1, 1),
                                    seasonal_order=(1, 1, 1, 12),
                                    enforce_stationarity=False,
                                    enforce_invertibility=False)
    #print('model created')

    # SAVE THE MODEL 
    #path = os.environ.get('my_path') #C:/Users/Cesare/OneDrive/studio/magistrale-data-science/big-data-tech/bdt_2021_project/'
    #modelname = '{river_name}-{variable}_model'.format(river_name = river_name, variable = variable) 
    #filename = path + modelname  
    #pickle.dump(mod, open(filename, 'wb'))

    #connection.close()
    #return one_step_df

    results = mod.fit()
    print('model trained')
    path = 'E:/' #os.environ.get('path_model') # 'E:/'     #os.environ.get('my_path') #C:/Users/Cesare/OneDrive/studio/magistrale-data-science/big-data-tech/bdt_2021_project/'
    modelname = '{river_name}-{variable}_model'.format(river_name = river_name, variable = variable) 
    filename = path +  modelname  
    pickle.dump(results, open(filename, 'wb'))
    #joblib.dump(results, filename, compress=('zlib', 6))

    #results.plot_diagnostics(figsize=(15, 12))
    #plt.show()    
    print('model saved')

    connection.close()

def prediction(modelname:str, variable: str, river_name:str):

    connection = mysql.connector.connect(
        host = os.environ.get('host'), #'ec2-18-117-169-228.us-east-2.compute.amazonaws.com', #'127.0.0.1'
        port =  3310,
        database = 'database_fiumi',  #'rivers_db'
        user = os.environ.get('user'), #root, user_new
        password = os.environ.get('password'), #password, passwordnew_user
        allow_local_infile = True
        )
    connection.autocommit = True

    path = 'E:/' #os.environ.get('path_model')  #'E:/'  #os.environ.get('my_path') 
    filename = path  + modelname
    results = pickle.load(open(filename, 'rb'))
    #results = joblib.load(filename)
    query = 'SELECT Timestamp, {variable} from {river_name}'.format(variable = variable,  river_name = river_name)
    df = pd.read_sql(query, con=connection)
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
        
        #output_l = str(pred_ci['lower {variable}_actual'.format(variable = variable)]).split()
        #output_u = str(pred_ci['upper {variable}_actual'.format(variable = variable)]).split()
        output_l = str(pred_ci['lower variable_actual']).split()
        output_u = str(pred_ci['upper variable_actual']).split()
        output = output_l[1] + ' - ' + output_u[1]
        list_output.append(output)

        '''
        plt.figure(figsize=(16,10), dpi=100)
        #ax = one_step_df.variable_actual[:].plot(label='observed')
        ax = df[variable][:].plot(label='observed')
        pred.predicted_mean.plot(ax=ax, label='Forecast')

        ax.fill_between(pred_ci.index,
                        pred_ci.iloc[:, 0],
                        pred_ci.iloc[:, 1], color='grey', alpha=1, label = 'confidence interval')

        ax.set_xlabel('Date')
        if variable == 'W_mean':
            ax.set_ylabel('Height (in Centimeters)')
        elif variable == 'Q_mean':
            ax.set_ylabel('Flow Rate m³/s')
        else:
            ax.set_ylabel('Temperature (in Celsius)')
        
        plt.legend()
        plt.xlim([start_pred -100,pred_1w + 50])
        #print(pred_ci.iloc[:, 0])
        #print(pred_ci.iloc[:, 1])
        #plt.show()  
        ''' 

    #connection.close()
    #print(list_output)

    path = os.environ.get('my_path')
    
    if river_name == 'Tabella_Isarco':
        id = 1
    elif river_name == 'Tabella_Adige':
        id = 2
    else:
        id = 3
    data = {'Timestamp':str(df.iloc[[-1]].Timestamp).split()[1]+' '+str(df.iloc[[-1]].Timestamp).split()[2],'1h':(float(list_output[0].split(' - ')[0]) + float(list_output[0].split(' - ')[1]) )/2, '3h':(float(list_output[1].split(' - ')[0]) + float(list_output[1].split(' - ')[1]) )/2, '12h':(float(list_output[2].split(' - ')[0]) + float(list_output[2].split(' - ')[1]) )/2, '1d':(float(list_output[3].split(' - ')[0]) + float(list_output[3].split(' - ')[1]) )/2, '3d':(float(list_output[4].split(' - ')[0]) + float(list_output[4].split(' - ')[1]) )/2, '1w':(float(list_output[5].split(' - ')[0]) + float(list_output[5].split(' - ')[1]) )/2, 'Id':id}
    dataframe = pd.DataFrame(data, index=[0])
    csvname = path+'/predictions_folder/{model}.csv'.format(model=modelname)
    dataframe.to_csv(csvname,index=False)

    connection.close()

def make_predictions():

    prediction('Tabella_Isarco-Q_mean_model', 'Q_mean', 'Tabella_Isarco')
    prediction('Tabella_Isarco-W_mean_model', 'W_mean', 'Tabella_Isarco')
    prediction('Tabella_Isarco-WT_mean_model', 'WT_mean', 'Tabella_Isarco')
    prediction('Tabella_Adige-Q_mean_model', 'Q_mean', 'Tabella_Adige')
    prediction('Tabella_Adige-W_mean_model', 'W_mean', 'Tabella_Adige')
    prediction('Tabella_Adige-WT_mean_model', 'WT_mean', 'Tabella_Adige')
    prediction('Tabella_Talvera-Q_mean_model', 'Q_mean', 'Tabella_Talvera')
    prediction('Tabella_Talvera-W_mean_model', 'W_mean', 'Tabella_Talvera')
    prediction('Tabella_Talvera-WT_mean_model', 'WT_mean', 'Tabella_Talvera')
    publisher_str('Previsioni completate, salvale!')
