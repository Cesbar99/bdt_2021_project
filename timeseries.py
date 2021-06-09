import mysql.connector
from mysql.connector import connection
import pandas as pd 
import matplotlib.pyplot as plt
from dateutil.parser import parse 
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
plt.rcParams.update({'figure.figsize': (10, 7), 'figure.dpi': 120})

connection = mysql.connector.connect(
        host= 'ec2-3-131-169-162.us-east-2.compute.amazonaws.com', #'127.0.0.1'
        port=  3310,
        database = 'test_databse',  #'rivers_db'
        user = 'root',
        password = 'password'
        )
connection.autocommit = True
cursor = connection.cursor()

## https://www.machinelearningplus.com/time-series/time-series-analysis-python/
## https://machinelearningmastery.com/time-series-forecasting-methods-in-python-cheat-sheet/

# GET THE DATA
tabelle_fiumi = ['Tabella_Isarco', 'Tabella_Adige', 'Tabella_Talvera']
variabili = [ 'W_mean', 'Wt_mean', 'Q_mean'] 
"""for tabella_fiume in tabelle_fiumi:
    for variabile in variabili:
        query = 'SELECT Timestamp, {dato} from {nome}'.format(dato = variabile, nome = tabella_fiume) 
        df = pd.read_sql(query, con=connection)"""

query = 'SELECT Timestamp, W_mean from Tabella_Isarco' 
df = pd.read_sql(query, con=connection)
print(df)

"""# VISUALIZE THE TIME SERIES 

def plot_df(df, x, y, title = "", xlabel='Date', ylabel='Value', dpi=100):
    plt.figure(figsize=(16,5), dpi=dpi)
    plt.plot(x, y, color='tab:red')
    plt.gca().set(title=title, xlabel=xlabel, ylabel=ylabel)
    plt.show()

# plot_df(df, x=df.index, y=df.value, title = 'Livello idrometico Isarco')

# SEASONAL PLOT OF A TIMESERIES 
'farlo meglio'
dati_2019 = {'date': [], 'value': []}
dati_2020 = {'date': [], 'value': []}
dati_2021 = {'date': [], 'value': []}
for el in df : 
    if '2019' in el[0]:
        dati_2019['date'].append(el[0])
        dati_2019['value'].append(el[1])
    elif '2020' in el[0]:
        dati_2020['date'].append(el[0])
        dati_2020['value'].append(el[1])
    else:
        dati_2021['date'].append(el[0])
        dati_2021['value'].append(el[0])

plt.plot(dati_2019['date'], dati_2019['value'])
plt.plot(dati_2020['date'], dati_2020['value'])
plt.plot(dati_2021['date'], dati_2021['value'])
plt.xlabel('date')
plt.ylabel('values')
plt.title('Livello idrometrico isarco nel corso degli anni')

# BOXPLOTS OF MOTH-WISE (SEASONAL) AND YEAR-WISE (TREND) DISTRIBUTION

# Prepare data
df['year'] = [d.year for d in df.date]
df['month'] = [d.strftime('%b') for d in df.date]
years = df['year'].unique()

# Draw Plot
fig, axes = plt.subplots(1, 2, figsize=(20,7), dpi= 80)
sns.boxplot(x='year', y='value', data=df, ax=axes[0])
sns.boxplot(x='month', y='value', data=df.loc[~df.year.isin([1991, 2008]), :])

# Set Title
axes[0].set_title('Year-wise Box Plot\n(The Trend)', fontsize=18); 
axes[1].set_title('Month-wise Box Plot\n(The Seasonality)', fontsize=18)
plt.show()"""