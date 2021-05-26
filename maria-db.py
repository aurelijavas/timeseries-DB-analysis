import time
import pandas as pd
import mariadb
from datetime import datetime
import os
import sys
import csv
from dotenv import load_dotenv

load_dotenv()

try:
   conn = mariadb.connect(
      user=os.getenv('MARIADB_USER'),
      password=os.getenv('MARIADB_PASSWORD'),
      host=os.getenv('MARIADB_HOST'),
      port=os.getenv('MARIADB_PORT'))
except (Exception, mariadb.Error) as error:
   print(f"Error connecting to MariaDB Platform: {error}")
   sys.exit(1)

def inserting_data():
   start = time.time()
   directory = ("time series/")
   for root,dirs,files in os.walk(directory):
      for file in files:
         if file.endswith(".csv"):
               data = pd.read_csv(root+file)

               cur = conn.cursor()
               
               SQL = "INSERT INTO timeseries(date, ticker, open, high, low, close, adj_close, vol) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
               for index, row in data.iterrows():
                  try:
                     cur.execute(SQL, (datetime.strptime(row['Date'], '%Y-%m-%d'), file[:-4].upper(), \
                                                         row['Open'], row['High'], \
                                                         row['Low'], row['Close'], \
                                                         row['Adj Close'], row['Volume']))
                  except (Exception, mariadb.Error) as error:
                     print('Error while inserting data:', error)
               conn.commit()
   conn.close()

   end = time.time()
   print(end - start)  

def querying_data(start_date, stop_date, time_interval = None):
   start = time.time()
   cur = conn.cursor()

   cur.execute(f'SELECT date, ticker, close FROM timeseries\
               WHERE date >= "{start_date}" AND date < "{stop_date}"')
   results = []

   if time_interval != None:
      with open('mariadb_results_'+time_interval+'.csv', mode='w') as output:
         output = csv.writer(output, delimiter='\n', quotechar='"', quoting=csv.QUOTE_MINIMAL)
         for (date, ticker, close) in cur:
            results.append(f"{date}, {ticker}, {close}")
         output.writerow(results)
   else:
      for (date, ticker, close) in cur:
         results.append(f"{date} {ticker} {close}")
      print("\n".join(results))

   end = time.time()
   print(end - start) 

def aggregating_data(start_date, stop_date, agg_function):
   start = time.time()
   cur = conn.cursor()

   cur.execute(f'SELECT ticker, {agg_function}(close) FROM timeseries\
               WHERE date >= "{start_date}" AND date < "{stop_date}"\
               GROUP BY ticker')
   results = []

   for (ticker, close) in cur:
      results.append(f"{ticker} {close}")
   print("\n".join(results))

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

