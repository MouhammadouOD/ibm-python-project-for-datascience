# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3 as sqlite
from datetime import datetime

url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
table_attribs = ["Country", "GDP_USD_billions"]
target_csv = 'Countries_by_GDP.csv'
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
sql_connection= sqlite.connect(db_name)
log_file = 'etl_project_log.txt'




def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    all_tables = soup.find_all('table')
    gdb_table = all_tables[2]
    all_rows = gdb_table.find_all('tr')
    df = pd.DataFrame(columns=table_attribs)
    #first_cell = all_rows.find_all('td')[0].a.contents[0]
    #print(first_cell)
    for i,row in enumerate(all_rows):
        if i >= 3:
            country = row.find_all('td')[0].a.contents[0]
            gdp = row.find_all('td')[2].contents[0]
            if not gdp or '—' in gdp or not country:
                continue
            df = pd.concat([df, pd.DataFrame([{"Country": country, "GDP_USD_billions": gdp}])], ignore_index=True)

    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    df['GDP_USD_billions'] = df['GDP_USD_billions'].str.replace(',', '').astype(float) / 1000
    df['GDP_USD_billions'] = df['GDP_USD_billions'].round(2)

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace')


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(pd.read_sql(query_statement, sql_connection))
    

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("Preliminaries complete. Initiating ETL process.")

#log_progress("Extraction step started")
extracted_data = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process.")

#log_progress("Transform step started")
transformed_data= transform(extracted_data)
log_progress("Data transformation complete. Initiating loading process")

#log_progress("Load step started")
load_to_csv(transform(extract(url, table_attribs)), target_csv)
log_progress("Data saved to CSV file.")

load_to_db(extracted_data, sql_connection, table_name)
log_progress("Data loaded to Database as table. Running the query.")

run_query(f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100", sql_connection)
log_progress("Process Complete.")

sql_connection.close()







