from datetime import timedelta, datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from extract_from_website import run_extract_from_website
from load_raw_data_to_s3 import run_load_raw_data
from extract_from_s3 import run_extract_from_s3
from transform import run_transform
from load_processed_data_to_s3 import run_load_processed_data
from copy_to_redshift import run_copy_to_redshift

dag_path = os.getcwd()

args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
}

config_dag = DAG(
    'create bucket S3 and Redshift Cluster',
    default_args = args,
    description = 'Cria o bucket S3 que servirá de staging e armazenará os dados processados, bem como cria um cluster gratuito no Redshift e cria uma tabela que receberá os registros para elaboração dos dashboards'
)

task = BashOperator(
    task_id = 'create_bucket_and_cluster',
    bash_command = f'{dag_path}/create-bucket-and-cluster.sh',
    dag = config_dag
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime.now() + timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

with DAG ('viagens_a_servico_dag',
          default_args = default_args,
          description = 'Dashboard dos dados de viagem do governo',
          schedule_interval = timedelta(weeks=4)) as data_pipeline_dag:
    

    task1 = PythonOperator(
        task_id = 'download_data_from_website',
        python_callable = run_extract_from_website,
        dag = data_pipeline_dag
    )

    task2 = PythonOperator(
        task_id = 'load_raw_data_to_AWS_S3',
        python_callable = run_load_raw_data,
        dag = data_pipeline_dag
    )

    task3 = PythonOperator(
        task_id = 'download_data_from_AWS_S3',
        python_callable = run_extract_from_s3,
        dag = data_pipeline_dag
    )

    task4 = PythonOperator(
        task_id = 'transform_data',
        python_callable = run_transform,
        dag = data_pipeline_dag
    )

    task5 = PythonOperator(
        task_id = 'load_processed_data_to_AWS_S3',
        python_callable = run_load_processed_data,
        dag = data_pipeline_dag
    )
    
    task6 = PythonOperator(
        task_id = 'copy_processed_data_to_a_Redshift_cluster',
        python_callable = run_copy_to_redshift,
        dag = data_pipeline_dag
    )

    # Fluxo de tarefas
    task1 >> task2 >> task3 >> task4 >> task5 >> task6