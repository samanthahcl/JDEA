from airflow.decorators import dag
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago
from datetime import timedelta

# --- IMPORTAÇÃO CORRIGIDA ---
from tasks_meuprojeto.extracao_meuprojeto import google_sheet_to_minio_etl 
from tasks_meuprojeto.silver_transformation import process_silver_layer 


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


# --- VARIÁVEIS DE CONFIGURAÇÃO DA DAG ---
SHEET_ID = '1qElDdURG1_c65sqOFvK0Wr3hqPT5fJkTnu5GYDaBNcs'
SHEET_NAME = 'Form Responses 1'
MINIO_ENDPOINT = 'http://minio:9000' 
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minio@1234!'
BUCKET_BRONZE = 'bronze-layer-mental-health'
BUCKET_SILVER = 'silver-layer-mental-health'
MARIADB_CONN_ID = 'mariadb_local' # ID da conexão configurada no Airflow


@dag(
    dag_id='dag_saude_mental_hibrida',
    default_args=default_args,
    description='Pipeline Híbrido (MinIO + MariaDB) Bronze e Silver para Dados de Saúde Mental',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['Medalhão', 'Híbrida', 'Saúde Mental', 'MariaDB'],
)
def main_dag_saude_mental():
    
    # 1. Task: Extração sheets -> Bronze (MinIO) E Carregamento no MariaDB
    task_bronze_ingestion = PythonOperator(
        task_id='task_bronze_extracao_hibrida',
        python_callable=google_sheet_to_minio_etl,
        op_args=[
            SHEET_ID, 
            SHEET_NAME, 
            BUCKET_BRONZE, 
            MINIO_ENDPOINT, 
            ACCESS_KEY, 
            SECRET_KEY,
            MARIADB_CONN_ID 
        ], 
    )

    # 2. Task: Transformação Bronze -> Silver (MinIO) E Carregamento no MariaDB
    task_silver_transformation = PythonOperator(
        task_id='task_silver_transformacao_hibrida',
        python_callable=process_silver_layer,
        op_args=[
            BUCKET_BRONZE, 
            BUCKET_SILVER, 
            SHEET_NAME, 
            MINIO_ENDPOINT, 
            ACCESS_KEY, 
            SECRET_KEY,
            MARIADB_CONN_ID
        ],
    )

    # 3. Fluxo de Dependência
    task_bronze_ingestion >> task_silver_transformation


# Instância da DAG
main_dag_instance = main_dag_saude_mental()