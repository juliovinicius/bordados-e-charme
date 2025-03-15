import time

import requests
from itertools import count

apikey1 = 'd2713e9ab254bee05f94f7d72a2cdc23d2df71e0370aefde2fd7b0b8b1c06338'
apikey2 = 'be5001dee4cad6ed552b50096dd022b132bf209417c5469b8360f395272cd74c'
apikey3 = '6f646fa987f2b6fa2bb8fd50024eba933c6befc0c0d37b395388b4107f34440b'
apikey4 = '882086a25329f3c81061baa3159f521df591d629aa4a57651b87f6ab180dd6b4'
apikey5 = 'dede5c75ea14c62541f2896a93b323baac894cb5655957bb6770cae4037f7319'
apikey6 = '3d4c4e3432ae1d59fdf6671d195efaee373f5d5f6105440bbfdaf49f52429299'

apis = [apikey1,
        apikey2,
        apikey3,
        apikey4]


def info_conta(apikey):
    url = ('https://api.tiny.com.br/api2/info.php?token=' +
           apikey +
           '&formato=json')

    response = requests.post(url=url).json()

    razao_social = response['retorno']['conta']['razao_social']

    return razao_social


def contas_a_receber(apikey):
    print('Extraindo contas a receber.')
    razao_social = info_conta(apikey)
    data_ini_emissao = '01/06/2024'
    data_fim_emissao = '30/06/2025'
    contas = []
    for i in count(1, step=1):
        url = (f'https://api.tiny.com.br/api2/contas.receber.pesquisa.php?token=' +
               apikey +
               '&formato=json&'
               f'&pagina={i}&data_ini_vencimento=' + data_ini_emissao + '&data_fim_vencimento=' + data_fim_emissao)

        response = requests.post(url=url).json()

        contas += response['retorno']['contas']
        print(f'Página {i} adicionada.')

        if int(response['retorno']['pagina']) >= response['retorno']['numero_paginas']:
            break

        time.sleep(2)

    tipo = 'receber'

    return contas, razao_social, apikey, tipo


def conta_a_receber(id_conta, apikey):
    url = ('https://api.tiny.com.br/api2/conta.receber.obter.php?token=' +
           apikey + '&id=' + id_conta + '&formato=json')

    response = requests.post(url=url).json()
    if 'codigo_erro' in response['retorno']:
        print(f'Erro ao acessar conta em busca da categoria.')
        if response['retorno']['codigo_erro'] == 6:
            print(f'O limite de requisições foi atingido. Tente novamente em alguns minutos.')
    conta = response['retorno']['conta']

    return conta


def contas_a_pagar(apikey):
    print('Extraindo contas a pagar.')
    razao_social = info_conta(apikey)
    data_ini_emissao = '01/06/2024'
    data_fim_emissao = '30/06/2025'
    contas = []
    for i in count(1, step=1):
        url = (f'https://api.tiny.com.br/api2/contas.pagar.pesquisa.php?token=' +
               apikey +
               '&formato=json&'
               f'&pagina={i}&data_ini_vencimento=' + data_ini_emissao + '&data_fim_vencimento=' + data_fim_emissao)

        response = requests.post(url=url).json()

        contas += response['retorno']['contas']
        print(f'Página {i} adicionada.')

        if int(response['retorno']['pagina']) >= response['retorno']['numero_paginas']:
            break

        time.sleep(2)

    tipo = 'pagar'

    return contas, razao_social, apikey, tipo


def conta_a_pagar(id_conta, apikey):
    url = ('https://api.tiny.com.br/api2/conta.pagar.obter.php?token=' +
           apikey + '&id=' + id_conta + '&formato=json')

    response = requests.post(url=url).json()
    if 'codigo_erro' in response['retorno']:
        print(f'Erro ao acessar conta em busca da categoria.')
        if response['retorno']['codigo_erro'] == 6:
            print(f'O limite de requisições foi atingido. Tente novamente em alguns minutos.')
    conta = response['retorno']['conta']

    return conta


if __name__ == '__main__':
    contas_a_receber(apikey6)
    #conta_a_receber('873427495', apikey1)
    #info_conta(apikey1)
    #contas_a_pagar(apikey2)
    #conta_a_pagar('869624779', apikey1)
