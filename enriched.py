import os
import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def lambda_handler(event: dict, context: dict) -> bool:
    '''
    Diariamente é executado para compactar as diversas mensagensm, no formato
    JSON, do dia anterior, armazenadas no bucket de dados cru, em um único
    arquivo no formato PARQUET, armazenando-o no bucket de dados enriquecidos
    '''

    # vars de ambiente

    RAW_BUCKET = os.environ['AWS_S3_BUCKET']
    ENRICHED_BUCKET = os.environ['AWS_S3_ENRICHED']

    # vars lógicas

    tzinfo = timezone(offset=timedelta(hours=-3))
    date = (datetime.now(tzinfo) - timedelta(days=1)).strftime('%Y-%m-%d')  # Carregar as mensagens D+1
    timestamp = datetime.now(tzinfo).strftime('%Y%m%d%H%M%S%f')

    # Esquema padronizado
    schema = pa.schema([
        ('message_id', pa.int64()),
        ('user_id', pa.int64()),
        ('user_is_bot', pa.bool_()),
        ('user_first_name', pa.string()),
        ('chat_id', pa.int64()),
        ('chat_type', pa.string()),
        ('date', pa.int64()),
        ('text', pa.string())
    ])

    # código principal

    table = None
    client = boto3.client('s3')

    try:
        response = client.list_objects_v2(Bucket=RAW_BUCKET, Prefix=f'telegram/context_date={date}')

        for content in response['Contents']:
            key = content['Key']
            client.download_file(RAW_BUCKET, key, f"/tmp/{key.split('/')[-1]}")

            with open(f"/tmp/{key.split('/')[-1]}", mode='r', encoding='utf8') as fp:
                data = json.load(fp)
                data = data["message"]

            parsed_data = parse_data(data=data)
            iter_table = pa.Table.from_pydict(mapping=parsed_data, schema=schema)

            if table:
                table = pa.concat_tables([table, iter_table])  # Concatenar novos dados caso haja uma tabela criada
            else:
                table = iter_table  # Senão criar uma nova tabela
                iter_table = None

        pq.write_table(table=table, where=f'/tmp/{timestamp}.parquet')
        client.upload_file(f"/tmp/{timestamp}.parquet", ENRICHED_BUCKET, f"telegram/context_date={date}/{timestamp}.parquet")

        return True

    except Exception as exc:
        logging.error(msg=exc)
        return False


# Código para função do Wrangling do .json
def parse_data(data: dict) -> dict:
    parsed_data = {
        'message_id': None,
        'user_id': None,
        'user_is_bot': None,
        'user_first_name': None,
        'chat_id': None,
        'chat_type': None,
        'date': None,
        'text': None,
    }

    for key, value in data.items():
        if key == 'from':
            for k, v in data[key].items():
                if k in ['id', 'is_bot', 'first_name']:
                    parsed_data[f"user_{k}"] = v

        elif key == 'chat':
            for k, v in data[key].items():
                if k in ['id', 'type']:
                    parsed_data[f"chat_{k}"] = v

        elif key in ['message_id', 'date', 'text']:
            parsed_data[key] = value

    return {k: [v] for k, v in parsed_data.items()}
