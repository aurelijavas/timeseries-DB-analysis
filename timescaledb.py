import pandas as pd
from datetime import datetime
import psycopg2
import time
import os
import csv
from dotenv import load_dotenv

load_dotenv()

CONNECTION = 'dbname='+os.getenv('TIMESCALEDB_NAME')+' user='+os.getenv('TIMESCALEDB_USER')+' password='+os.getenv('TIMESCALEDB_PASSWORD')+' host=localhost port=5432 sslmode=require'


#1st and 2nd cases
def inserting_data():
    start = time.time()
    directory = ("time series/")
    for root,dirs,files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                data = pd.read_csv(root+file)
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
                conn.close()

    end = time.time()
    print(end - start)  

def querying_data(start_date, stop_date, time_interval = None):
    start = time.time()

    with psycopg2.connect(CONNECTION) as conn:
        cur = conn.cursor()
        
        cur.execute(f"SELECT date, ticker, close FROM timeseries\
               WHERE date >= '{start_date}' AND date < '{stop_date}'")
        results = []

        if time_interval != None:
            with open('timescaledb_results_'+time_interval+'.csv', mode='w') as output:
                output = csv.writer(output, delimiter='\n', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for (date, ticker, close) in cur:
                    results.append(f"{date}, {ticker}, {close}")
                output.writerow(results)
        else:
            for (date, ticker, close) in cur:
                results.append(f"{date} {ticker} {close}")
            print("\n".join(results))

        cur.close()
    conn.close()

    end = time.time()
    print(end - start) 

def aggregating_data(start_date, stop_date, agg_function):
    start = time.time()

    with psycopg2.connect(CONNECTION) as conn:
        cur = conn.cursor()

        cur.execute(f"SELECT ticker, {agg_function}(close) FROM timeseries\
                WHERE date >= '{start_date}' AND date < '{stop_date}'\
                GROUP BY ticker")
        results = []

        for (ticker, close) in cur:
            results.append(f"{ticker} {close}")
        
        print("\n".join(results))

        cur.close()
    conn.close()

    end = time.time()
    print(end - start) 

inserting_data()

querying_data('2014-05-22', '2014-05-23') #1 day
querying_data('2014-05-22', '2014-05-29') #1 week
querying_data('2014-05-22', '2014-06-22', 'month') #1 month
querying_data('2014-05-22', '2015-05-22', 'year') #1 year

aggregating_data('2014-05-22', '2014-06-22', 'avg') #1 month
aggregating_data('2014-05-22', '2015-05-22', 'avg') #1 year
aggregating_data('2014-05-22', '2014-06-22', 'sum') #1 month
aggregating_data('2014-05-22', '2015-05-22', 'sum') #1 year


