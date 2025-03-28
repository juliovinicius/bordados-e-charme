import pandas as pd
from pathlib import Path

CAMINHO_BASE = Path(__file__).parent / 'contas.parquet'


def teste():
    base = pd.read_parquet(CAMINHO_BASE)

    base['DATA_LEITURA'] = pd.to_datetime(base['DATA_LEITURA'], errors='coerce')
    base['DATA_LEITURA'] = pd.Timestamp('2000-01-01')

    base.to_parquet(CAMINHO_BASE, index=False)

    return base


def transformando_vencimento():
    base = pd.read_parquet(CAMINHO_BASE)

    base['VENCIMENTO'] = pd.to_datetime(base['VENCIMENTO'], format='%d/%m/%Y', errors='coerce')
    base['EMISSÃO'] = pd.to_datetime(base['EMISSÃO'], format='%d/%m/%Y', errors='coerce')
    base['LIQUIDAÇÃO'] = pd.to_datetime(base['LIQUIDAÇÃO'], format='%d/%m/%Y', errors='coerce')

    base.to_parquet(CAMINHO_BASE, index=False)

    return base


if __name__ == '__main__':
    transformando_vencimento()