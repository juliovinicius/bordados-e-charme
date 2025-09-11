import extratores.blingv3
from extratores.google_cloud_storage import get_storage_client, BLOB_TOKEN_BLING
import transformadores
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
import os
from pathlib import Path
import pickle
import json


CAMINHO_JSON = str(Path(__file__).parent.parent /'credenciais'/'chave-google.json')

BUCKET = 'bling_bcharm'
PARQUET_NOVOS_BLOB = 'pedidos_a_preencher.parquet'
PARQUET_GERAL_BLOB = 'pedidos.parquet'
BQ_DATASET = 'layer2'
BQ_TABLE = 'pedidos'
BLOB_TOKEN_BLING_JSON = 'bling_v3_access_token.json'


def salvando_no_gcs(df_novo: pd.DataFrame, df_total: pd.DataFrame):
    client = get_storage_client(CAMINHO_JSON)
    bucket = client.bucket(BUCKET)
    blob_novos = bucket.blob(PARQUET_NOVOS_BLOB)
    blob_total = bucket.blob(PARQUET_GERAL_BLOB)

    buffer = io.BytesIO()
    df_novo.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_novos.upload_from_file(buffer, rewind=True)
    print('Arquivo incremental salvo no GCS.')

    buffer = io.BytesIO()
    df_total.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_total.upload_from_file(buffer, rewind=True)
    print('Arquivo geral salvo no GCS.')

    return 'Salvos no GCS.'


def salvar_bling_token(tokens):
    client = get_storage_client(CAMINHO_JSON)
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(BLOB_TOKEN_BLING_JSON)

    blob.upload_from_string(tokens, content_type="application/json")

    return 'Token salvo no GCS'


def salvando_no_bigquery(base: pd.DataFrame):
    print("Enviando para BigQuery...")
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # ou WRITE_APPEND se preferir
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    buffer = io.BytesIO()
    base.to_parquet(buffer, index=False)
    buffer.seek(0)

    job = bq_client.load_table_from_file(
        buffer,
        destination=f"{bq_client.project}.{BQ_DATASET}.{BQ_TABLE}",
        job_config=job_config
    )
    job.result()
    print(f"Tabela {BQ_DATASET}.{BQ_TABLE} atualizada com sucesso.")

    return f'Tabela {BQ_DATASET}.{BQ_TABLE} atualizada com sucesso'


if __name__ == '__main__':
    df_novo, df_total = transformadores.estrutura_pedido.multiplos_pedidos(
        extratores.blingv3.pedidos_gerais()
    )
    salvando_no_gcs(df_novo, df_total)
