from itertools import count
from pathlib import Path

import requests as requests
import json
import os

APIKEY = '5a6ac722be7b259de8b6f8d1cb5c6d52d4e5116dc256dfa894f9631f0c58d47df55591ae'
PEDIDOS = str(Path(__file__).parent.parent / "arquivos" / "pedidos.json")


def carregar_pedidos(arquivo):
    if os.path.exists(arquivo):
        with open(arquivo, 'r') as arquivo_tmp:
            return json.load(arquivo_tmp)
    else:
        return []


def salvar_pedidos(arquivo, pedidos):
    print(f'\n\nSalvando arquivo em {arquivo}.')
    with open(arquivo, 'w') as arquivo_tmp:
        json.dump(pedidos, arquivo_tmp)


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
    pedidos = carregar_pedidos(PEDIDOS)
    ids_existentes = {pedido['pedido']['numero'] for pedido in pedidos}
    filtro = 'dataEmissao[01/03/2024 TO 16/03/2024]'
    for i in count(1, step=1):
        resposta = requests.get(f"https://bling.com.br/Api/v2/pedidos/page={i}/json/",
                                params={'apikey': apikey,
                                        'filters': filtro}).json()
        if 'erros' in resposta['retorno'].keys():
            codigo_do_erro = resposta['retorno']['erros'][0]['erro']['cod']
            if codigo_do_erro == 14:
                break
        else:
            novos_pedidos = resposta['retorno']['pedidos']
            novos_pedidos_filtrados = [pedido for pedido in novos_pedidos if
                                       pedido['pedido']['numero'] not in ids_existentes]
            if novos_pedidos_filtrados:
                pedidos.extend(novos_pedidos_filtrados)
                ids_existentes.update(pedido['pedido']['numero'] for pedido in novos_pedidos_filtrados)

            if i in (50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800):
                print(f'\n\nIds existentes: {len(ids_existentes)}\n\n')

                print(f'\n\nPágina {i} adicionada.\n\n')

    '''resposta = requests.get(f"https://bling.com.br/Api/v2/pedidos/page=1/json/",
                                params={'apikey': apikey,
                                        'filters': filtro}).json()
    pedidos += resposta['retorno']['pedidos']'''

    print('Pedidos extraídos.\n')
    print(f'\n\nIds existentes: {len(ids_existentes)}\n\n')

    salvar_pedidos(PEDIDOS, pedidos)

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


def todas_as_integracoes_lojas_virtuais(apikey: str = APIKEY):
    lojas = []

    response = requests.options(f"https://bling.com.br/Api/v2/pedidos/page=1/json/",
                                params={'apikey': apikey}).json()

    return response


def ler_nota_fiscal(id_nota, apikey: str = APIKEY):
    response = requests.get(f'https://bling.com.br/Api/v2/nfe/{id_nota}',
                            params={'apikey': apikey}).json()

    return response


if __name__ == '__main__':
    ler_nota_fiscal(20969866836)
