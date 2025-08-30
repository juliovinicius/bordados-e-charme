from google.cloud import storage
import pandas as pd
import io
import os
from pathlib import Path

BUCKET = 'bling_bcharm'
PARQUET_GERAL_BLOB = 'pedidos.parquet'


def ler_arquivo_no_gcs():
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(PARQUET_GERAL_BLOB)

    if not blob.exists():
        print("Nenhum arquivo encontrado no GCS. Retornando DataFrame vazio.")
        return pd.DataFrame(columns=['id', 'data_de_atualizacao'])

    buffer = io.BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)

    df = pd.read_parquet(buffer, engine="pyarrow")
    print(f"{len(df)} linhas carregadas do GCS.")
    return df

