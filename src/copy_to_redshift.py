import psycopg2
import datetime as dt
import os

today = dt.date.today()
s3_file = f's3://viagens-a-servico-gov/{today.year}/{today.month}/processed-data/viagens-processed-data.csv'

host = ''
port = ''
database = 'viagens-DW'
user = ''

dag_path = os.getcwd()

with open(dag_path + '/password.txt', 'r') as f:
    password = f.read()

with open(dag_path + '/credentials.txt', 'r') as f:
    credentials = f.read()
    access_key, secret_key = credentials.split(',')

def run_copy_to_redshift():

    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    cursor = conn.cursor()

    copy_query = f'''
        COPY fatoviagens
        FROM '{s3_file}'
        credentials
        'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
        region 'sa-east-1'
        delimiter ','
        DATE FORMAT 'DD/MM/YYYY'
    '''

    cursor.execute(copy_query)
    conn.commit()