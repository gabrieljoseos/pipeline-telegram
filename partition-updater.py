import boto3
import os
from datetime import datetime, timedelta

# Configurações do AWS
ACCESS_KEY = os.environ['access_key']
SECRET_KEY = os.environ['secret_key']
REGION_NAME = os.environ['region']

# Nome do bucket e prefixo da tabela
BUCKET_NAME = os.environ['bucket_name']
TABLE_PREFIX = os.environ['table_prefix']

# Configurando a sessão do boto3
session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION_NAME
)

s3 = session.resource('s3')
athena = session.client('athena')

def lambda_handler(event, context):
    # Sua lógica de atualização de partições aqui
    update_partitions()

# Função para obter a data do dia anterior
def get_yesterday_date():
    return (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

# Função para atualizar partições
def update_partitions():
    yesterday_date = get_yesterday_date()
    query = f"ALTER TABLE telegram ADD IF NOT EXISTS PARTITION (context_date='{yesterday_date}') LOCATION 's3://{BUCKET_NAME}/{TABLE_PREFIX}/context_date={yesterday_date}/'"

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'default'
        },
        ResultConfiguration={
            'OutputLocation': f's3://{BUCKET_NAME}/athena-results/'
        }
    )

    print(f"Query execution started: {response['QueryExecutionId']}")
