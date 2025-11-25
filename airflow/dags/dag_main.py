#from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta

from __pycache__.tasks.g_sheets_extracao_dk import google_sheet_to_minio_etl
from __pycache__.tasks.transformacao_silver_dk  import process_silver_layer

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
    dag_id='dag_main',
    default_args=default_args,
    description='Dag curso JDEA',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['Bikeronaldinho','extração','transformação', 'etl', 'raw'],
)


def main_dag():
    #table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']  # Altere conforme necessário
    google_sheets = ['Clientes', 'Vendedores', 'Produtos', 'Vendas', 'ItensVendas']  # Altere para o nome das abas da sua planilha
    #bucket_name = 'raw'
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'
    sheet_id = '1XMrFMPptCKfy6u2tsP0FuAgPlbJ-anA0caH2e6LL5F0'
    bucket_bronze = 'raw'
    bucket_silver = 'silver'

     # TaskGroup: Extração do Google Sheets para camada Bronze (MinIO)
    with TaskGroup("extracao_g_sheets_dk", tooltip="Tasks processadas do google sheets para minio, salvando em .parquet") as extracao_g_sheets_dk:
        for sheets_name in google_sheets:
            PythonOperator(
                task_id=f'task_sheets_{sheets_name}',
                python_callable=google_sheet_to_minio_etl,
                op_args=[sheet_id, sheets_name, bucket_bronze, endpoint_url, access_key, secret_key]
            )

    with TaskGroup("transformacao_silver_dk", tooltip="Transformação da camada Bronze para Silver") as transformacao_silver_dk:
        for sheets_name in google_sheets:
            PythonOperator(
                task_id=f'transform_{sheets_name}',
                python_callable=process_silver_layer,
                op_args=[bucket_bronze, bucket_silver, sheets_name, endpoint_url, access_key, secret_key]
            )

    # Fluxo de dependência
    extracao_g_sheets_dk >> transformacao_silver_dk


main_dag_instance = main_dag()
