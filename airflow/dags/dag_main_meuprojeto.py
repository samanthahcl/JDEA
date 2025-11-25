#from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta

from tasks_meuprojeto.extracao_meuprojeto import postgres_to_minio_etl_parquet_full

# Argumentos iniciais.
default_args = {
    'owner': 'samantha',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Parametro para dags
@dag(
    dag_id='dag_main_dados',
    default_args=default_args,
    description='Dag curso JDEA - Dados Novadrive',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['NovaDrive', 'extração', 'transformação', 'etl', 'raw'],
)


def main_dag():
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']  # Altere conforme necessário
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'
    bucket_bronze = 'raw'
    #bucket_silver = 'silver'

    # TaskGroup: Extração do Google Sheets para camada Bronze (MinIO)
    with TaskGroup("group_task_parquet_full", tooltip="Tasks processadas do BD externo para minio, salvando em .parquet") as group_task_parquet_full:
        for table_name in table_names:
            PythonOperator(
                task_id=f'task_parquet_full_{table_name}',
                python_callable=postgres_to_minio_etl_parquet_full,
                op_args=[table_name, bucket_bronze, endpoint_url, access_key, secret_key]
            )
    # Fluxo de dependência
    group_task_parquet_full


main_dag_instance = main_dag()