# Code for ETL operations on bank data

# Importing the required libraries
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3 as sqlite
from datetime import datetime

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
target_csv = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'
exchange_rate_path = './exchange_rate.csv'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all('table')[0]
    tbody = table.tbody
    rows = tbody.find_all('tr')
    df = pd.DataFrame(columns=table_attribs)
    for i,row in enumerate(rows):
        if i >= 1:
            name = row.find_all('td')[1].contents[2].contents[0]
            mc_usd_billion = row.find_all('td')[2].contents[0].split('\n')[0]
            df = pd.concat([df, pd.DataFrame([{"Name":name, "MC_USD_Billion": mc_usd_billion}])], ignore_index=True)
    #print(df)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_data = pd.read_csv(csv_path)
    exchange_rate = exchange_data.set_index('Currency')['Rate'].to_dict()
    df['MC_GBP_Billion'] = [np.round(float(x)*float(exchange_rate['GBP']),2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(float(x)*float(exchange_rate['EUR']),2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(float(x)*float(exchange_rate['INR']),2) for x in df['MC_USD_Billion']]

    print(df)

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(f"Query Statement : {query_statement}")
    output = pd.read_sql(query_statement, sql_connection)
    print(f"Output : {output}")


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


log_progress("Preliminaries complete. Initiating ETL process")

extracted_data = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

transformed_data = transform(extracted_data, exchange_rate_path)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(transformed_data, target_csv)
log_progress("Data saved to CSV file")

sql_connection= sqlite.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(transformed_data, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

log_progress("Print the contents of the entire table")
read_entire_table = 'SELECT * FROM Largest_banks'
run_query(read_entire_table, sql_connection)

log_progress("Print the average market capitalization of all the banks in Billion USD.")
avg_mc_in_billion_usd = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(avg_mc_in_billion_usd, sql_connection)

log_progress("Print only the names of the top 5 banks")
read_top_5_banks_names = 'SELECT Name from Largest_banks LIMIT 5'
run_query(read_top_5_banks_names, sql_connection)

log_progress("Queries sucessfully executed")

log_progress("Process Complete")

log_progress("Server Connection closed")
