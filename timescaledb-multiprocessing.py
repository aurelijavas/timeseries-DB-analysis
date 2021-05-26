from datetime import datetime
import pandas as pd
import os
import time
import psycopg2
from multiprocessing import Pool
from dotenv import load_dotenv

load_dotenv()

def inserting_data(file):
    if file.endswith(".csv"):
        data = pd.read_csv(directory+file)
        with psycopg2.connect(CONNECTION) as conn:
            cur = conn.cursor()
            SQL = "INSERT INTO timeseries_thread (date, ticker, open, high, low, close, adj_close, vol) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
            for index, row in data.iterrows():
                try:
                    data_insert = (datetime.strptime(row['Date'], '%Y-%m-%d'), file[:-4].upper(), \
                                    row['Open'], row['High'], \
                                    row['Low'], row['Close'], \
                                    row['Adj Close'], row['Volume'])
                    cur.execute(SQL, data_insert)
                except (Exception, psycopg2.Error) as error:
                    print('Error while inserting data:', error, data_insert)                
            conn.commit()
            cur.close()
            # print("5")
        conn.close()
    return


CONNECTION = 'dbname='+os.getenv('TIMESCALEDB_NAME')+' user='+os.getenv('TIMESCALEDB_USER')+' password='+os.getenv('TIMESCALEDB_PASSWORD')+' host=localhost port=5432 sslmode=require'
directory = ("time series/")

if __name__ == '__main__':
    start = time.time()
    for root,dirs,files in os.walk(directory):
        with Pool(len(files)) as p:
            p.map(inserting_data, files)
            
                    
    end = time.time()
    print(end - start)