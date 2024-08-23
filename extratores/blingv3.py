import base64
import pickle
from datetime import datetime, timedelta
from itertools import count

import requests
from pathlib import Path


CAMINHO_BLING_ACCESS_TOKEN = Path(__file__).parent.parent / "cache" / "bling_v3_access_token.b"
client_id = '6a98683078ddd386e7702e995261f604ddca8a72'
client_secret = '64e8d1ad698d75e3e1f40e6d94773b11417b4580d961bbdb292dcd5c3b3a'
url_padrao = 'https://bling.com.br/Api/v3'


def get_bling_access_token():
    if CAMINHO_BLING_ACCESS_TOKEN.exists():
        with open(CAMINHO_BLING_ACCESS_TOKEN, 'rb') as token_file:
            bling_access_token_data = pickle.load(token_file)
        if datetime.now() < bling_access_token_data['expires_at']:
            print(f'O código de acesso armazenado ainda é válido.\n'
                  f'Código: {bling_access_token_data["access_token"]}.')
            return bling_access_token_data['access_token']

        print('O código de acesso está expirado.\nIniciando processo de atualização.')

        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': '1.0',
            'Authorization': f'Basic {encoded_credentials}'
        }

        data = {
            'grant_type': 'refresh_token',
            'refresh_token': bling_access_token_data['refresh_token']
        }

        url = 'https://www.bling.com.br/Api/v3/oauth/token'

        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 200:
            print('Token de acesso atualizado com sucesso.')
            tokens = response.json()
            created_at = datetime.now()
            expires_at = created_at + timedelta(seconds=21600)
            tokens["created_at"] = created_at
            tokens["expires_at"] = expires_at
            print(f'O novo código expira em: {tokens["expires_at"]}')
            with open(CAMINHO_BLING_ACCESS_TOKEN, 'wb') as token_file:
                pickle.dump(tokens, token_file)

            return tokens['access_token']
        else:
            print(f"Failed to refresh token. Status code: {response.status_code}")
            print(response.text)

    print('sem arquivo')
    return 'sem arquivo'


def pedidos_gerais():
    access_token = get_bling_access_token()

    '''
    exemplo de filtros:
    params = {
    'pagina': 1,
    'limite': 100,
    'idContato': '12345678',
    'idsSituacoes[]': ['12345678', '87654321'],
    'dataInicial': '2022-01-01',
    'dataFinal': '2022-01-15',
    'dataAlteracaoInicial': '2022-01-01',
    'dataAlteracaoFinal': '2022-01-15',
    'dataPrevistaInicial': '2022-01-01',
    'dataPrevistaFinal': '2022-01-15',
    'numero': '12345',
    'idLoja': '12345678',
    'idVendedor': '12345678',
    'idControleCaixa': '12345678'
    }'''

    pedidos = []
    dt = datetime.now()
    data_inicial = (dt - timedelta(days=180)).strftime('%Y-%m-%d')
    data_alteracao_inicial = (dt - timedelta(days=1)).strftime('%Y-%m-%d')

    for i in count(1, step=1):
        resposta = requests.get(url=f'{url_padrao}/pedidos/vendas',
                               headers={
                                   'Authorization': f'Bearer {access_token}'
                               },
                               params={
                                   'pagina': f'{i}',
                                   'dataInicial': data_inicial,
                                   'dataAlteracaoInicial': data_alteracao_inicial
                               }).json()

        pedidos += resposta['data']

        if len(resposta['data']) == 0:
            break

    return pedidos


def obter_pedido(numero_do_pedido: int):
    access_token = get_bling_access_token()

    resposta = requests.get(
        url=f'{url_padrao}/pedidos/vendas/{numero_do_pedido}',
        headers={
            'Authorization': f'Bearer {access_token}'
        }
    ).json()
    print(f'Requisição concluída para o pedido de id {numero_do_pedido}.')

    return resposta


def contatos_gerais():
    access_token = get_bling_access_token()

    contatos = []
    '''for i in count(1, step=1):
        resposta = requests.get(url=f'{url_padrao}/contatos',
                                headers={
                                    'Authorization': f'Bearer {access_token}'
                                },
                                params={
                                    'pagina': f'{i}',
                                    'dataAlteracaoInicial': '16/06/2024'
                                }).json()

        contatos += resposta['data']

        if len(resposta['data']) == 0:
            break'''

    resposta = requests.get(url=f'{url_padrao}/contatos',
                            headers={
                                'Authorization': f'Bearer {access_token}'
                            },
                            params={
                                'pagina': 1,
                                'dataAlteracaoInicial': '2024-06-15',
                                'dataAlteracaoFinal': '2024-06-15'
                            }).json()

    return resposta


if __name__ == '__main__':
    obter_pedido(20978032803)
