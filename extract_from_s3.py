import s3fs
import pandas as pd

with open('credentials.txt', 'r') as f:
    credentials = f.read()
    aws_access_key, aws_secret_key = credentials.split(',')

fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)

bucket_name = 'viagens-a-servico-gov/2023/raw-data'

files = ['2023_Pagamento.csv', '2023_Passagem.csv', '2023_Trecho.csv', '2023_Viagem.csv']

for file in files:
    with fs.open(f'{bucket_name}/{file}', 'rb') as f:
        with open(f'new-{file}', 'wb') as file:
            file.write(f.read())