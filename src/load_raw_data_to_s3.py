import s3fs
import pandas as pd
import os
import datetime as dt

today = dt.date.today()
dag_path = os.getcwd()

with open(dag_path + '/credentials.txt', 'r') as f:
    credentials = f.read()
    aws_access_key, aws_secret_key = credentials.split(',')

s3_uri = f's3://viagens-a-servico-gov/raw-data/'

# Lista de arquivos a serem enviados para o s3
files = ['2023_Pagamento.csv', '2023_Passagem.csv', '2023_Trecho.csv', '2023_Viagem.csv']

def run_load_raw_data():
    
    # Cria uma instância do sistema de arquivos S3
    fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
    
    dfs = []

    try:
    
        for file in files:
            df = pd.read_csv(fs.open(s3_uri + file))
            dfs.append(df)
    
    except:

        # Itera sobre a lista de arquivos e faz o upload para o S3
        for local_file in files:

            with fs.open(s3_uri + local_file, 'wb') as s3_file:
                with open(f'{dag_path}/{local_file}', 'rb') as file:
                    s3_file.write(file.read())

            os.remove(f'{dag_path}/{local_file}')

    else:

        for i in range(4):
            df_local = pd.read_csv(f'{dag_path}/{files[i]}')
            df_s3 = dfs[i]
            df_union = pd.concat([df_local, df_s3])
            df_union = df_union.drop_duplicates()

            with fs.open(s3_uri + files[i], 'wb') as s3_file:
                with open(f'{dag_path}/{files[i]}', 'rb') as file:
                    s3_file.write(file.read())