from datetime import datetime
import time
import os
from multiprocessing import Pool
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

token = os.getenv('INFLUXDB_TOKEN')
org = os.getenv('INFLUXDB_ORG')
bucket = os.getenv('INFLUXDB_BUCKET')

client = InfluxDBClient(url=os.getenv('INFLUXDB_URL'), token=token)

def inserting_data(file):
    if file.endswith(".csv"):
        data = pd.read_csv(directory+file)
        for index, row in data.iterrows():
            try:
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
            except Exception as error:
                    print('Error while inserting data:', error, file[:-4].upper(), row['Date'], row['Volume']) 

    return


directory = ("time series/")
write_api = client.write_api(write_options=SYNCHRONOUS)
threads = []

if __name__ == '__main__':
    start = time.time()

    for root,dirs,files in os.walk(directory):
        with Pool(len(files)) as p:
            p.map(inserting_data, files)
                    
    end = time.time()
    print(end - start)