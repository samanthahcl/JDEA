import pandas as pd
import boto3
import io
import logging
from botocore.exceptions import ClientError


# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_data(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    logger.info(f"Iniciando transformações para a planilha: {sheet_name}")

    if sheet_name == "Clientes":
        df.columns = [col.strip().lower() for col in df.columns]
        df['sexo'] = df['sexo'].str.upper()
        df['status'] = df['status'].str.capitalize()
        df.dropna(subset=["clienteid", "cliente"], inplace=True)

    elif sheet_name == "Produtos":
        df.columns = [col.strip().lower() for col in df.columns]
        df['preco'] = df['preco'].astype(float)

    elif sheet_name == "Vendas":
        df.columns = [col.strip().lower() for col in df.columns]
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
        df['total'] = df['total'].astype(float)

    elif sheet_name == "ItensVendas":
        df.columns = [col.strip().lower() for col in df.columns]
        df[['quantidade', 'valorunitario', 'valortotal', 'desconto', 'totalcomdesconto']] = \
            df[['quantidade', 'valorunitario', 'valortotal', 'desconto', 'totalcomdesconto']].apply(pd.to_numeric, errors='coerce')

    else:
        logger.warning(f"Nenhuma transformação específica definida para: {sheet_name}")

    return df

def process_silver_layer(bucket_bronze: str, bucket_silver: str, sheet_name: str, endpoint_url: str, access_key: str, secret_key: str):
    logger.info(f"Iniciando processamento da camada Silver para: {sheet_name}")

    # Conecta ao MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        # Lê os dados da camada Bronze
        response = minio_client.get_object(Bucket=bucket_bronze, Key=f"{sheet_name}/data.parquet")
        df = pd.read_parquet(io.BytesIO(response['Body'].read()))
        logger.info(f"Leitura concluída da camada Bronze para: {sheet_name}")

        # Aplica transformações
        df_transformed = transform_data(df, sheet_name)

        # Verifica se o bucket Silver existe; cria se não existir
        existing_buckets = minio_client.list_buckets()
        bucket_names = [b['Name'] for b in existing_buckets['Buckets']]
        if bucket_silver not in bucket_names:
            logger.warning(f"Bucket '{bucket_silver}' não encontrado. Criando novo bucket.")
            minio_client.create_bucket(Bucket=bucket_silver)
            logger.info(f"Bucket '{bucket_silver}' criado com sucesso.")

        # Salva na camada Silver
        parquet_buffer = io.BytesIO()
        df_transformed.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(
            Bucket=bucket_silver,
            Key=f"{sheet_name}/data_silver.parquet",
            Body=parquet_buffer.getvalue()
        )
        logger.info(f"Arquivo salvo com sucesso na camada Silver para: {sheet_name}")

    except Exception as e:
        logger.error(f"Erro durante o processamento da camada Silver: {e}")
        raise
