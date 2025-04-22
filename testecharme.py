import pandas as pd
from pathlib import Path
import requests
import json
from itertools import count

CAMINHO_CONTAS = Path(__file__).parent / "cache" / "contas.parquet"

apikey1 = 'd2713e9ab254bee05f94f7d72a2cdc23d2df71e0370aefde2fd7b0b8b1c06338'
apikey2 = 'be5001dee4cad6ed552b50096dd022b132bf209417c5469b8360f395272cd74c'
apikey3 = '6f646fa987f2b6fa2bb8fd50024eba933c6befc0c0d37b395388b4107f34440b'
apikey4 = '882086a25329f3c81061baa3159f521df591d629aa4a57651b87f6ab180dd6b4'

apis = [apikey1,
        apikey2,
        apikey3,
        apikey4]


def teste():
    data_ini_emissao = '01/01/2024'
    data_fim_emissao = '31/12/2024'
    #for i in count(1, step=1):
    url = (f'https://api.tiny.com.br/api2/contas.receber.pesquisa.php?token=' +
           apikey1 +
           '&formato=json&'
           f'&pagina={2}&data_ini_emissao=' + data_ini_emissao + '&data_fim_emissao=' + data_fim_emissao)

    response = requests.post(url=url).json()

    if int(response['retorno']['pagina']) <= response['retorno']['numero_paginas']:
        check = 1
    else:
        check = 0

    '''if int(response['retorno']['pagina']) <= response['retorno']['numero_paginas']:
        for conta in response['retorno']['contas']:
            dados_da_conta = {
                'ID': conta['conta']['id'],
                'CLIENTE': conta['conta']['nome_cliente'],
                'HISTORICO': conta['conta']['historico'],
                'EMISSÃO': conta['conta']['data_emissao'],
                'VENCIMENTO': conta['conta']['data_vencimento'],
                'VALOR': conta['conta']['valor'],
                'SALDO': conta['conta']['saldo'],
                'SITUAÇÃO': conta['conta']['situacao']
            }'''


    return response


def teste2(id_conta):
    url = ('https://api.tiny.com.br/api2/conta.receber.obter.php?token=' +
           apikey1 + '&id=' + id_conta + '&formato=json')

    response = requests.post(url=url).json()

    return response


def teste_contas_pagar():
    data_ini_emissao = '01/01/2024'
    data_fim_emissao = '31/12/2024'
    url = ('https://api.tiny.com.br/api2/contas.pagar.pesquisa.php?token=' +
           apikey +
           '&formato=json&data_ini_emissao=' + data_ini_emissao + '&data_fim_emissao=' + data_fim_emissao)

    response = requests.post(url=url).json()

    return response


def infoconta(apikey: str):
    url = ('https://api.tiny.com.br/api2/info.php?token=' +
           apikey +
           '&formato=json')

    response = requests.post(url=url).json()

    razao_social = response['retorno']['conta']['razao_social']

    return razao_social


def acrescentando_backup():
    base = pd.read_parquet(CAMINHO_CONTAS)
    base['HISTÓRICO'] = ''
    base.to_parquet(CAMINHO_CONTAS)

    return base


if __name__ == '__main__':
    #teste()
    #teste2('873399173')
    #teste_contas_pagar()
    #infoconta(apikey4)
    acrescentando_backup()
