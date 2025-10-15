import time
from dateutil.relativedelta import relativedelta
from calendar import monthrange
import requests
from itertools import count
from datetime import date

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

hoje = date.today()

# Data inicial: 1º dia de 3 meses atrás
data_ini = (hoje.replace(day=1) - relativedelta(months=3))
data_ini_emissao = data_ini.strftime('%d/%m/%Y')

# Data final: último dia de 4 meses à frente
data_fim = hoje.replace(day=1) + relativedelta(months=4)
ultimo_dia_fim = monthrange(data_fim.year, data_fim.month)[1]
data_fim_emissao = data_fim.replace(day=ultimo_dia_fim).strftime('%d/%m/%Y')


def info_conta(apikey):
    url = ('https://api.tiny.com.br/api2/info.php?token=' +
           apikey +
           '&formato=json')

    response = requests.post(url=url).json()

    razao_social = response['retorno']['conta']['razao_social']

    return razao_social


def contas_a_receber(apikey, data_ini_emissao, data_fim_emissao):
    print('Extraindo contas a receber.')
    razao_social = info_conta(apikey)
    contas = []
    for i in count(1, step=1):
        url = (f'https://api.tiny.com.br/api2/contas.receber.pesquisa.php?token=' +
               apikey +
               '&formato=json&'
               f'&pagina={i}&data_ini_vencimento=' + data_ini_emissao + '&data_fim_vencimento=' + data_fim_emissao)

        response = requests.post(url=url).json()

        retorno = response.get('retorno', {})
        if retorno.get('codigo_erro') == 20 or 'contas' not in retorno:
            print(f'Sem contas a receber para o período. Código de erro: {retorno.get("codigo_erro")}')
            break

        contas += response['retorno']['contas']
        print(f'Página {i} adicionada.')

        if int(response['retorno']['pagina']) >= response['retorno']['numero_paginas']:
            break

        time.sleep(3)

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


def contas_a_pagar(apikey, data_ini_emissao, data_fim_emissao):
    print('Extraindo contas a pagar.')
    razao_social = info_conta(apikey)
    contas = []
    for i in count(1, step=1):
        url = (f'https://api.tiny.com.br/api2/contas.pagar.pesquisa.php?token=' +
               apikey +
               '&formato=json&'
               f'&pagina={i}&data_ini_vencimento=' + data_ini_emissao + '&data_fim_vencimento=' + data_fim_emissao)

        response = requests.post(url=url).json()

        retorno = response.get('retorno', {})
        if retorno.get('codigo_erro') == 20 or 'contas' not in retorno:
            print(f'Sem contas a receber para o período. Código de erro: {retorno.get("codigo_erro")}')
            break

        contas += response['retorno']['contas']
        print(f'Página {i} adicionada.')

        if int(response['retorno']['pagina']) >= response['retorno']['numero_paginas']:
            break

        time.sleep(3)

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
    #contas_a_receber(apikey6)
    conta_a_receber('942035553', apikey2)
    #info_conta(apikey1)
    #contas_a_pagar(apikey2)
    #conta_a_pagar('942035553', apikey3)
