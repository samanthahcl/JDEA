import gspread
import pandas as pd
from io import BytesIO
from minio import Minio
from google.oauth2.service_account import Credentials
import os # Importar para usar os.path.join ou os.makedirs, embora não usado diretamente aqui

# === CONFIGURAÇÕES ===
GOOGLE_SHEET_ID = '1WlTYC9MjmfPWSkEPLSevZqlz07-wEluis7i_B-wLnbQ' # O ID da sua planilha Google Sheets

# ATENÇÃO: Caminho do JSON no seu sistema Windows. O 'r' é crucial para evitar erros de escape.
# Certifique-se de que este caminho corresponde ao local real do seu arquivo credentials.json.
CREDENTIALS_PATH = r'C:\Users\saman\OneDrive\Desktop\jdata\JDEA\airflow\config_airflow\credentials.json'

# ATENÇÃO: MINIO_ENDPOINT alterado para 'localhost:9000' para execução local no Windows.
# Se este script fosse executado DENTRO do Docker, seria 'minio:9000'.
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'minioadmin'
# ATENÇÃO: MINIO_SECRET_KEY corrigida para 'minio@1234!' conforme seu docker-compose.yml.
MINIO_SECRET_KEY = 'minio@1234!'
BUCKET_NAME = 'planilhas' # Nome do bucket onde os arquivos serão salvos no MinIO

# === AUTENTICAÇÃO COM GOOGLE SHEETS ===
# Define o escopo de acesso (apenas leitura de planilhas)
scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Carrega as credenciais da conta de serviço a partir do arquivo JSON
# Isso permite que o script se autentique com a API do Google Sheets.
creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scope)

# Autoriza o cliente gspread a usar as credenciais
client = gspread.authorize(creds)

# Abre a planilha Google Sheets usando o ID fornecido
spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

# === CONEXÃO COM MINIO ===
# Inicializa o cliente Minio
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False # 'secure=False' é usado para conexões HTTP (não-HTTPS) em ambientes de desenvolvimento
)

# Verifica se o bucket existe no MinIO e o cria se não existir
if not minio_client.bucket_exists(BUCKET_NAME):
    print(f"Criando bucket '{BUCKET_NAME}' no MinIO...")
    minio_client.make_bucket(BUCKET_NAME)
else:
    print(f"Bucket '{BUCKET_NAME}' já existe no MinIO.")

# === EXPORTAR TODAS AS ABAS PARA MINIO ===
print("\nIniciando exportação das abas do Google Sheets para o MinIO...")
for worksheet in spreadsheet.worksheets(): # Itera sobre cada aba na planilha
    nome_aba = worksheet.title # Obtém o título da aba atual
    print(f"Exportando aba: {nome_aba}")

    try:
        dados = worksheet.get_all_records() # Extrai todos os dados da aba como uma lista de dicionários
    except gspread.exceptions.GSpreadException as e:
        # Captura e lida com erros específicos do gspread, como cabeçalhos duplicados
        print(f"AVISO: Erro ao ler a aba '{nome_aba}' (pode ser cabeçalhos duplicados): {e}. Pulando esta aba.")
        continue # Pula para a próxima aba se houver um erro de leitura

    df = pd.DataFrame(dados) # Converte os dados extraídos em um DataFrame pandas

    if df.empty: # Verifica se o DataFrame está vazio
        print(f"Aba {nome_aba} está vazia. Pulando...")
        continue # Pula para a próxima aba se estiver vazia

    # Converter o DataFrame para CSV em um buffer de memória
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False) # Converte para CSV sem incluir o índice do DataFrame
    csv_buffer.seek(0) # Volta o ponteiro do buffer para o início para que possa ser lido

    # Enviar o arquivo CSV do buffer para o MinIO
    try:
        minio_client.put_object(
            BUCKET_NAME,         # Nome do bucket de destino
            f"{nome_aba}.csv",   # Nome do objeto (arquivo) no MinIO (ex: "Clientes_Bike.csv")
            csv_buffer,          # O buffer de memória contendo os dados CSV
            length=csv_buffer.getbuffer().nbytes, # Tamanho do objeto em bytes (necessário para put_object)
            content_type='text/csv' # Tipo de conteúdo do arquivo
        )
        print(f"Aba {nome_aba} enviada com sucesso para o MinIO!")
    except Exception as e:
        print(f"ERRO ao enviar a aba '{nome_aba}' para o MinIO: {e}")
        # Você pode adicionar um 'raise' aqui se quiser que o script pare em caso de erro no upload
        # raise

print("\n✅ Processo concluído.")
