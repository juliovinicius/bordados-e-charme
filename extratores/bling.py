from itertools import count

import requests as requests

APIKEY = '5a6ac722be7b259de8b6f8d1cb5c6d52d4e5116dc256dfa894f9631f0c58d47df55591ae'


def todos_os_produtos(apikey: str = APIKEY):
    produtos = []
    for i in count(1, step=1):
        resposta = requests.get(f"https://bling.com.br/Api/v2/produtos/page={i}/json/",
                                params={'apikey': apikey, 'estoque': 'S'}).json()
        if 'erros' in resposta['retorno'].keys():
            codigo_do_erro = resposta['retorno']['erros'][0]['erro']['cod']
            if codigo_do_erro == 14:
                break
        else:
            produtos += resposta['retorno']['produtos']
            print(f'Página {i} adicionada.\n')

    '''resposta = requests.get(f"https://bling.com.br/Api/v2/produtos/page=1/json/",
                            params={'apikey': apikey, 'estoque': 'S'}).json()
    produtos += resposta['retorno']['produtos']'''

    print('Produtos extraídos.\n')

    return produtos


def todos_os_pedidos(apikey: str = APIKEY):
    pedidos = []
    for i in count(1, step=1):
        resposta = requests.get(f"https://bling.com.br/Api/v2/pedidos/page={i}/json/",
                                params={'apikey': apikey}).json()
        if 'erros' in resposta['retorno'].keys():
            codigo_do_erro = resposta['retorno']['erros'][0]['erro']['cod']
            if codigo_do_erro == 14:
                break
        else:
            pedidos += resposta['retorno']['pedidos']
            print(f'Página {i} adicionada.\n')

    '''resposta = requests.get(f"https://bling.com.br/Api/v2/pedidos/page=1/json/",
                                params={'apikey': apikey}).json()
    pedidos += resposta['retorno']['pedidos']'''

    print('Pedidos extraídos.\n')

    #tentar usar filtros no request pra quebrar a extração em partes por ano, tipo:

    '''import requests

    apikey = 'your_api_key_here'
    page_number = 1
    filters = 'dataEmissao[01/01/2023 TO 31/01/2023]'

    response = requests.get(
        f"https://bling.com.br/Api/v2/pedidos/page={page_number}/json/",
        params={
            'apikey': apikey,
            'filters': filters,
            'sort': 'dataEmissao',
            'fields': 'numero,dataEmissao,cliente,nome,valorTotal'
        }
    )

    if response.status_code == 200:
        response_json = response.json()
        print(response_json)
    else:
        print(f"Failed to retrieve data: {response.status_code}")'''

    return pedidos


if __name__ == '__main__':
    todos_os_pedidos()
