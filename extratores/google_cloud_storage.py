from google.cloud import storage
import pandas as pd
import io
import os
from pathlib import Path
from google.auth import default as google_default_credentials
import pickle

BUCKET = 'bling_bcharm'
PARQUET_GERAL_BLOB = 'pedidos.parquet'
BLOB_TOKEN_BLING = 'bling_v3_access_token.b'
BLOB_TOKEN_BLING_JSON = 'bling_v3_access_token.json'
CAMINHO_JSON = str(Path(__file__).parent.parent / 'credenciais'/ 'chave-google.json')


def gcp_credenciais(caminho_json: str = None):
    """
        Configura as credenciais do GCP.

        - local_json_path: caminho opcional para JSON local (apenas para desenvolvimento).
        """
    # Se estiver rodando localmente e o JSON existir, define a variável de ambiente
    if caminho_json:
        json_path = Path(caminho_json)
        if json_path.exists():
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(json_path)
            print(f"[INFO] Usando credenciais locais: {json_path}")
            return

    # Caso contrário, tenta pegar credenciais padrão do ambiente
    try:
        creds, project = google_default_credentials()
        print(f"[INFO] Usando credenciais padrão do ambiente (project={project})")
    except Exception as e:
        raise RuntimeError(
            "Não foi possível localizar credenciais do GCP. "
            "Para desenvolvimento local, forneça o caminho do JSON. "
            "No Cloud Run, verifique a conta de serviço."
        ) from e


def get_storage_client(caminho_json: str = None):
    """
        Retorna um client do GCS já autenticado.
        """
    gcp_credenciais(caminho_json)
    return storage.Client()


def ler_arquivo_no_gcs():
    client = get_storage_client(CAMINHO_JSON)
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

def ler_bling_token():
    client = get_storage_client(CAMINHO_JSON)
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(BLOB_TOKEN_BLING)

    if not blob.exists():
        print('Arquivo de token de acesso não encontrado no bucket.')
        return

    buffer = io.BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)
    bling_access_token_data = pickle.load(buffer)

    return bling_access_token_data

