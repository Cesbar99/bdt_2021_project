import pandas as pd 
import numpy as np 
import os 
from pandas import *
import mysql.connector
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

    from statsmodels.tsa.seasonal import seasonal_decompose

    # Additive Decomposition
    result_add = seasonal_decompose(df[variable], model='additive', extrapolate_trend='freq', freq=365)

    # Shift the current temperature to the next day. 
    predicted_df = df[variable].to_frame().shift(1).rename(columns = {variable: "variable_pred" })
    actual_df = df[variable].to_frame().rename(columns = {variable: "variable_actual" })

    # Concatenate the actual and predicted temperature
    one_step_df = pd.concat([actual_df,predicted_df],axis=1)

    # Select from the second row, because there is no prediction for today due to shifting.
    one_step_df = one_step_df[1:]

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

    results = mod.fit()
    print('model trained')
    path = 'E:/'   
    #path = os.environ.get('my_path') 
    modelname = '{river_name}-{variable}_model'.format(river_name = river_name, variable = variable) 
    filename = path +  modelname  
    joblib.dump(results, filename)
    print('model saved')  

    connection.close()

def prediction(modelname:str, variable: str, river_name:str, dataframe):

    path = 'E:/'  
    #path = os.environ.get('my_path')  
    filename = path  + modelname
    results = joblib.load(filename)
    df = dataframe
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
        output_l = str(pred_ci['lower variable_actual']).split()
        output_u = str(pred_ci['upper variable_actual']).split()
        output = output_l[1] + ' - ' + output_u[1]
        list_output.append(output)

    path = os.environ.get('my_path')
    
    if river_name == 'Tabella_Isarco':
        id = 1
    elif river_name == 'Tabella_Adige':
        id = 2
    else:
        id = 3
    name = river_name+'-'+variable
    #data = {'Timestamp':str(df.iloc[[-1]].Timestamp).split()[1]+' '+str(df.iloc[[-1]].Timestamp).split()[2],'1h':(float(list_output[0].split(' - ')[0]) + float(list_output[0].split(' - ')[1]) )/2, '3h':(float(list_output[1].split(' - ')[0]) + float(list_output[1].split(' - ')[1]) )/2, '12h':(float(list_output[2].split(' - ')[0]) + float(list_output[2].split(' - ')[1]) )/2, '1d':(float(list_output[3].split(' - ')[0]) + float(list_output[3].split(' - ')[1]) )/2, '3d':(float(list_output[4].split(' - ')[0]) + float(list_output[4].split(' - ')[1]) )/2, '1w':(float(list_output[5].split(' - ')[0]) + float(list_output[5].split(' - ')[1]) )/2, 'Id':id}
    
    data = {'Timestamp':str(df.iloc[[-1]][df.columns[0]]).split()[1]+' '+str(df.iloc[[-1]][df.columns[0]]).split()[2],'1h':list_output[0], '3h':list_output[1], '12h':list_output[2], '1d':list_output[3], '3d':list_output[4], '1w':list_output[5], 'Id':id}
    dataframe = pd.DataFrame(data, index=[0])
    csvname = path+'/predictions_folder/{pred}.csv'.format(pred=name+'_prediction')
    dataframe.to_csv(csvname,index=False)


