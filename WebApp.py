import streamlit as st 
import pandas as pd 
from PIL import Image

# Add a title and an image
st.write("""
# Stock market web application
**Visually** show data on a stock! Data range from Jan 2, 2020- Aug 4 2020.
""")

image = Image.open('C:/Users/alber/Desktop/UniTn/Data Science/Second_Semester/Big_data/Project/map.png')
st.image(image, use_column_width= True)

# Create side bar header
st.sidebar.header('User Input')
'''
# Create a function to get the users input
def get_input():
    start_date = st.sidebar.text_input('Start Date', '2019-02-28 00:00:00')
    end_date = st.sidebar.text_input('End Date', '2021-06-20 23:00:00')
    River_symbol = st.sidebar.text_input('River Symbol', 'ISARCO')
    return start_date, end_date, River_symbol

def get_river_name(symbol):
    if symbol == 'ISARCO':
        return 'Isarco'
    elif symbol == 'TALVERA':
        return 'Talvera'
    elif symbol == 'ADIGE':
        return 'Adige'
    else: 
        None

# Create a function to get the proper river data and the proper timeframe from the user start date to the user end data
def get_data(symbol, start, end):
     
     # Load the data 
    if symbol.upper() == 'ISARCO':
        df = pd.read_csv('')
    
    elif symbol.upper() == 'ISARCO':
        df = pd.read_csv('')
    
    elif symbol.upper() == 'ISARCO':
        df = pd.read_csv('')
    else:
        df = pd.DataFrame(columns=['Timestamp', 'Variable'])

    # Get the data range 
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)

    # Set the start and end index rows both to 0 
    start_row = 0 
    end_row = 0

    # Start the date from the data set and go down to see if the users start date is less than or equal to the data in the data set
    for i in range(0 ,len(df)):
        if start <= pd.to_datetime(df['Timestamp'][i]):
            start_row = i 
            break 

    # Start from the bottom of the data set and go up to see if the users end data is greater than or equalto the date into the data set 

    for j in range(0, len(df)):
        if end >= pd.to_datetime(df['Date'][len(df)-1-j]):
            end_row = len(df) - 1 - j 
            break 

    # Set the index to 
    df = df.set_index(pd.DatetimeIndex(df['Date'].values))

    return df.iloc[start_row:end_row + 1]

# Get the users input
start, end, symbol = get_input()
# Get data 
df = get_data(symbol, start, end)
# Get the company name
company_name = get_river_name(symbol.upper())

# Display the close price 
st.header(company_name + 'Close Price\n')
st.line_chart(df['Close'])

# Display the close price 
st.header(company_name + 'Volume\n')
st.line_chart(df['Volume'])

# Get statistics 
st.header('Data Statistics')
st.write(df.describe())
'''