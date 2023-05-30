import s3fs

with open('credentials.txt', 'r') as f:
    credentials = f.read()
    aws_access_key, aws_secret_key = credentials.split(',')

bucket_name = 'viagens-a-servico-gov/2023/processed-data'

local_file = 'viagem-processed-data.csv'

fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)

s3_file_path = f's3://{bucket_name}/{local_file}'

with fs.open(s3_file_path, 'wb') as s3_file:
    with open(local_file, 'rb') as file:
        s3_file.write(file.read())