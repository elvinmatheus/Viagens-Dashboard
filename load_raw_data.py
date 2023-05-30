import s3fs
import pandas as pd

with open('credentials.txt', 'r') as f:
    credentials = f.read()
    aws_access_key, aws_secret_key = credentials.split(',')

bucket_name = 'viagens-a-servico-gov/2023/raw-data'

# Lista de arquivos a serem enviados para o s3
local_files = ['2023_Pagamento.csv', '2023_Passagem.csv', '2023_Trecho.csv', '2023_Viagem.csv']

# Cria uma instância do sistema de arquivos S3
fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)

# Itera sobre a lista de arquivos e faz o upload para o S3
for local_file in local_files:
    s3_file_path = f's3://{bucket_name}/{local_file}'

    with fs.open(s3_file_path, 'wb') as s3_file:
        with open(local_file, 'rb') as file:
            s3_file.write(file.read())