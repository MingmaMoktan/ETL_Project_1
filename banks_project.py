# Code for ETL operations on Country-GDP data
'''
Correct flow of ETL:
-load_to_csv() → saves CSV
-Create database connection → sqlite3.connect()
-load_to_db() → saves DataFrame to SQL table
-Close connection → sql_connection.close()
'''

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

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    log_progress('Starting Transformation process')

    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate = dict(zip(exchange_rate_df['Currency'], exchange_rate_df['Rate']))

    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate.get('GBP', 1), 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate.get('EUR', 1), 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate.get('INR', 1), 2)

    log_progress('Data transformation complete. Initiating Loading process')
    return df


df = transform(df, './exchange_rate.csv')
print(df)


# Print 5th bank's EUR market cap (for quiz)
print("5th largest bank MC_EUR_Billion:", df['MC_EUR_Billion'][4])


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index=False)  # index=False to avoid adding extra column in CSV
    log_progress('Data saved to CSV file')
    
output_path = './largest_bank.csv'
load_to_csv(df, output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress('Data loaded to Database as a table, Executing queries')

db_name = 'Banks.db'
table_name = 'Largest_banks'

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(f"Running query: {query_statement}")
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    log_progress(f"Executed query: {query_statement}")

''' 
Here, you define the required entities and call the relevant
functions in the correct order to complete the project. 
Note that this
portion is not inside any function.'''


query1 = "SELECT * FROM Largest_banks"
run_query(query1, sql_connection)

query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query2, sql_connection)

query3 = "SELECT Name FROM Largest_banks LIMIT 5"
run_query(query3, sql_connection)

sql_connection.close()
log_progress('Server Connection closed')
