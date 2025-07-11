!pip install beautifulsoup4
!pip install requests
!pip install pandas

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
from google.colab import files

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = './Largest_banks_data.csv'
rate_csv = './exchange_rate.csv'
table_name = 'Largest_banks'

query_statement1 = f"SELECT * from {table_name}"
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
query_statement3 = f"SELECT Name from {table_name} LIMIT 5"

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            a_tags = col[1].find_all('a')
            if a_tags[1] is not None and '—' not in col[1]:
                name = a_tags[1].contents[0]
                market_cap = col[2].contents[0].strip().replace('\n', '')
                market_cap = float(market_cap)
                data_dict = {"Name": name, "MC_USD_Billion": market_cap}
                df1 = pd.DataFrame([data_dict])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df, csv_path):
    exchange_rate = pd.read_csv(rate_csv)

    dict = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_EUR_Billion'] = [np.round(x*dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*dict['INR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x*dict['GBP'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
  df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

run_query(query_statement1, sql_connection)

log_progress('Process Complete.')

log_progress('Data loaded to Database as a table, Executing queries')

run_query(query_statement2, sql_connection)

log_progress('Process Complete.')

log_progress('Data loaded to Database as a table, Executing queries')

run_query(query_statement3, sql_connection)

log_progress('Process Complete.')

sql_connection.close()

log_progress('Server Connection closed')
