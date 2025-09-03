import extratores.blingv3
from extratores.google_cloud_storage import get_storage_client, BLOB_TOKEN_BLING
import transformadores
from google.cloud import storage
import pandas as pd
import io
import os
from pathlib import Path
import pickle


CAMINHO_JSON = str(Path(__file__).parent.parent /'credenciais'/'chave-google.json')

BUCKET = 'bling_bcharm'
PARQUET_NOVOS_BLOB = 'pedidos_a_preencher.parquet'
PARQUET_GERAL_BLOB = 'pedidos.parquet'


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
    blob = bucket.blob(BLOB_TOKEN_BLING)

    buffer = io.BytesIO()
    pickle.dump(tokens, buffer)
    buffer.seek(0)
    blob.upload_from_file(buffer, rewind=True)

    return 'Token salvo no GCS'


if __name__ == '__main__':
    df_novo, df_total = transformadores.estrutura_pedido.multiplos_pedidos(
        extratores.blingv3.pedidos_gerais()
    )
    salvando_no_gcs(df_novo, df_total)
