import extratores.blingv3
import transformadores
from google.cloud import storage
import pandas as pd
import io


BUCKET = 'bling_bcharm'
PARQUET_BLOB = 'pedidos_a_preencher.parquet'


def salvando_no_gcs(df: pd.DataFrame):
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(PARQUET_BLOB)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob.upload_from_file(buffer, rewind=True)
    print('Arquivo incremental salvo no GCS.')

    return 'Salvo no GCS.'


if __name__ == '__main__':
    salvando_no_gcs(transformadores.estrutura_pedido.multiplos_pedidos(extratores.blingv3.pedidos_gerais()))
