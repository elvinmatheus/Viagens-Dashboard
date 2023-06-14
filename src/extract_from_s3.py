import s3fs
import pandas as pd
import os
import datetime as dt

today = dt.date.today()
dag_path = os.getcwd()

with open(dag_path + '/credentials.txt', 'r') as f:
    credentials = f.read()
    aws_access_key, aws_secret_key = credentials.split(',')

s3_uri = f's3://viagens-a-servico-gov/{today.year}/{str(today.month).zfill(2)}/raw-data/'

def run_extract_from_s3():

    fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)

    file_list =fs.ls(s3_uri)

    sorted_files = sorted(file_list, key=lambda x: fs.info(x)['LastModified'], reverse=True)

    num_files = 4

    for i in range(num_files):
        file_path = sorted_files[i]
        file_name = file_path.split('/')[-1]
        local_file_name = f'{dag_path}/raw-data/{file_name}'
        fs.get(file_path, local_file_name)