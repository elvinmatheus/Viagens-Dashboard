import s3fs
import datetime as dt
import os

today = dt.date.today()

dag_path = os.getcwd()

with open(dag_path + '/credentials.txt', 'r') as f:
    credentials = f.read()
    aws_access_key, aws_secret_key = credentials.split(',')

s3_uri = f's3://viagens-a-servico-gov/{today.year}/{str(today.month).zfill(2)}/processed-data/'

local_file = 'viagens-processed-data.csv'

def run_load_processed_data():

    fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)

    with fs.open(s3_uri + local_file, 'wb') as s3_file:
        with open(f'{dag_path}/processed-data/{local_file}', 'rb') as file:
            s3_file.write(file.read())