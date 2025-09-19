# Code for ETL operations on Country-GDP data


# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

# Defining required entities
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

log_file = "code_log.txt"

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            bank_name = cols[1].text.strip()
            market_cap = cols[2].text.strip()
            market_cap = market_cap.replace('\n', '').replace(',', '')
            market_cap = float(market_cap)
            data_dict = {"Name": bank_name, "MC_USD_Billion": market_cap}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
print(df)


import pandas as pd
import numpy as np

def transform(df, exchange_rate_path):
    """
    Transform the extracted DataFrame by converting MC_USD_Billion
    to MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion using exchange rates.
    """
    log_progress('Starting Transformation process')
    
    # Read CSV using the function argument
    exchange_rate_df = pd.read_csv(exchange_rate_path)
    exchange_rate = dict(zip(exchange_rate_df['Currency'], exchange_rate_df['Rate']))

    # Add new columns with rounded values
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    
    log_progress('Data transformation complete. Initiating Loading process')
    
    return df

# Make sure this is below the function definition
df = transform(df, './data/exchange_rate.csv')
print(df)

# Print 5th bank's EUR market cap (for quiz)
print("5th largest bank MC_EUR_Billion:", df['MC_EUR_Billion'][4])


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''