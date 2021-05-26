from datetime import datetime
import time
import os
import csv
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

load_dotenv()

token = os.getenv('INFLUXDB_TOKEN')
org = os.getenv('INFLUXDB_ORG')
bucket = os.getenv('INFLUXDB_BUCKET')

client = InfluxDBClient(url=os.getenv('INFLUXDB_URL'), token=token)

def inserting_data():
    start = time.time()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    directory = ("time series/")
    for root,dirs,files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                data = pd.read_csv(root+file)

                for index, row in data.iterrows():
                
                    point = Point('stock') \
                        .tag('ticker', file[:-4].upper()) \
                        .field('open', row['Open']) \
                        .field('high', row['High']) \
                        .field('low', row['Low']) \
                        .field('close', row['Close']) \
                        .field('adj_close', row['Adj Close']) \
                        .field('vol', row['Volume']) \
                        .time(datetime.strptime(row['Date'], '%Y-%m-%d'), WritePrecision.NS)

                    write_api.write(bucket, org, point)

    end = time.time()
    print(end - start)

def querying_data(start_date, stop_date, time_interval = None):
    start = time.time()

    query = f'from(bucket:"{bucket}")\
    |> range(start: {start_date}, stop: {stop_date})\
    |> filter(fn:(r) => r._measurement == "stock")\
    |> filter(fn:(r) => r._field == "close" )'
    result = client.query_api().query(org=org, query=query)
    results = []

    if time_interval != None:
        with open('influxdb_results_'+time_interval+'.csv', mode='w') as output:
            output = csv.writer(output, delimiter='\n', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for table in result:
                for record in table.records:
                    results.append((str(record.get_time()), record.values.get('ticker'), record.get_value()))
            output.writerow(results)
    else:
        for table in result:
            for record in table.records:
                results.append((str(record.get_time()), record.values.get('ticker'), record.get_value()))
        print(results)

    end = time.time()
    print(end - start)

def aggregating_data(start_date, stop_date, agg_function):
    start = time.time()

    query = f'from(bucket:"{bucket}")\
    |> range(start: {start_date}, stop: {stop_date})\
    |> filter(fn:(r) => r._measurement == "stock")\
    |> filter(fn:(r) => r._field == "close" )\
    |> {agg_function}\
    |> group(columns: ["_measurement"], mode:"by")'
    result = client.query_api().query(org=org, query=query)
    results = []

    for table in result:
        for record in table.records:
            results.append((record.values.get('ticker'), record.get_value()))
    print(results)

    end = time.time()
    print(end - start)

inserting_data()

querying_data('2014-05-22T00:00:00Z', '2014-05-23T00:00:00Z') #1 day
querying_data('2014-05-22T00:00:00Z', '2014-05-29T00:00:00Z') #1 week
querying_data('2014-05-22T00:00:00Z', '2014-06-22T00:00:00Z', 'month') #1 month
querying_data('2014-05-22T00:00:00Z', '2015-05-22T00:00:00Z', 'year') #1 year

aggregating_data('2014-05-22T00:00:00Z', '2014-06-22T00:00:00Z', 'mean()') #1 month
aggregating_data('2014-05-22T00:00:00Z', '2015-05-22T00:00:00Z', 'mean()') #1 year
aggregating_data('2014-05-22T00:00:00Z', '2014-06-22T00:00:00Z', 'sum()') #1 month
aggregating_data('2014-05-22T00:00:00Z', '2015-05-22T00:00:00Z', 'sum()') #1 year
