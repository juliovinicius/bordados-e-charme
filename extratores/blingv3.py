import base64
import pickle
from datetime import datetime, timedelta
from itertools import count
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import time

import requests
from pathlib import Path


CAMINHO_BLING_ACCESS_TOKEN = Path(__file__).parent.parent / "cache" / "bling_v3_access_token.b"
CAMINHO_PARA_CREDENCIAIS_DO_SHEETS = Path(__file__).parent.parent / "credenciais" / "chave-google-sheets.json"
client_id = '6a98683078ddd386e7702e995261f604ddca8a72'
client_secret = '64e8d1ad698d75e3e1f40e6d94773b11417b4580d961bbdb292dcd5c3b3a'
url_padrao = 'https://bling.com.br/Api/v3'

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(CAMINHO_PARA_CREDENCIAIS_DO_SHEETS, scope)
client = gspread.authorize(credentials)


def get_bling_access_token():
    if CAMINHO_BLING_ACCESS_TOKEN.exists():
        with open(CAMINHO_BLING_ACCESS_TOKEN, 'rb') as token_file:
            bling_access_token_data = pickle.load(token_file)
        if datetime.now() < bling_access_token_data['expires_at']:
            print(f'O código de acesso armazenado ainda é válido.')
            return bling_access_token_data['access_token']

        print('O código de acesso está expirado.\nIniciando processo de atualização.')

        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        print(f'1º {credentials}\n\n2º {encoded_credentials}')

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
    data_inicial = (dt - timedelta(days=60)).strftime('%Y-%m-%d')
    data_alteracao_inicial = (dt - timedelta(days=26)).strftime('%Y-%m-%d')
    data_alteracao_final = (dt - timedelta(days=21)).strftime('%Y-%m-%d')

    for i in count(1, step=1):
        resposta = requests.get(url=f'{url_padrao}/pedidos/vendas',
                               headers={
                                   'Authorization': f'Bearer {access_token}'
                               },
                               params={
                                   'pagina': f'{i}',
                                   'dataInicial': data_inicial,
                                   'dataAlteracaoInicial': data_alteracao_inicial
                                   #,'dataAlteracaoFinal': data_alteracao_final
                               }).json()

        pedidos += resposta['data']

        if len(resposta['data']) == 0:
            break

    return pedidos


def produtos_gerais():
    access_token = get_bling_access_token()
    print('Lendo produtos.')

    produtos = []
    dt = datetime.now()
    data_inicial = (dt - timedelta(days=15)).strftime('%Y-%m-%d %H:%M:%S')
    data_final = (dt + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    data_alteracao_inicial = (dt - timedelta(days=60)).strftime('%Y-%m-%d %H:%M:%S')
    data_alteracao_final = (dt + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    for i in count(1, step=1):
        print(f'página {i}')
        resposta = requests.get(url=f'{url_padrao}/produtos',
                               headers={
                                   'Authorization': f'Bearer {access_token}'
                               },
                               params={
                                   'pagina': f'{i}'
                                   ,'dataInclusaoInicial': data_inicial,
                                   #'dataInclusaoFinal': data_final,
                                   'dataAlteracaoInicial': data_alteracao_inicial
                                   #,'dataAlteracaoFinal': data_alteracao_final
                               }).json()

        produtos += resposta['data']
        time.sleep(0.3)
        print(f'requisição realizada para a página {i}')

        if len(resposta['data']) == 0:
            break

    return produtos


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


def ler_nota_fiscal(id_nota):
    access_token = get_bling_access_token()
    dados_da_nota = requests.get(
        url=f'{url_padrao}/nfe/{id_nota}',
        headers={
            'Authorization': f'Bearer {access_token}'
        }
    ).json()
    tipo_da_nota = dados_da_nota['data']['tipo']
    contato_da_nota = dados_da_nota['data']['contato']
    numero_da_nota = dados_da_nota['data']['numero']
    print(f'tipo da nota: {tipo_da_nota}\ncontato da nota: {contato_da_nota}')

    return dados_da_nota, tipo_da_nota, contato_da_nota, numero_da_nota


def ler_planilha(url, id):
    planilha = client.open_by_key(url).get_worksheet_by_id(id)
    dados = planilha.get("C6:F")

    df = pd.DataFrame(dados)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df['valor'] = df['valor'].str.replace(',', '.')
    df['codigo'] = df['codigo'].str.upper()
    df = df.to_dict(orient="records")

    return df


def alterar_nota_fiscal(id_nota, url_da_planilha, id_da_planilha):
    access_token = get_bling_access_token()
    datahora_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(datahora_atual)

    dados_da_nota, tipo_da_nota, contato_da_nota, numero_da_nota = ler_nota_fiscal(id_nota)
    contato_da_nota['tipoPessoa'] = 'J'
    contato_da_nota['contribuinte'] = 9
    print(f'contato da nota: {contato_da_nota}')

    itens_a_colocar_na_nota = ler_planilha(url_da_planilha, id_da_planilha)

    response = requests.put(url=f'{url_padrao}/nfe/{id_nota}',
                            headers={
                                'Authorization': f'Bearer {access_token}'
                            },
                            json={
                                'tipo': tipo_da_nota,
                                'numero': numero_da_nota,
                                'dataOperacao': datahora_atual,
                                'contato': contato_da_nota,
                                'itens': itens_a_colocar_na_nota,
                                'naturezaOperacao': {'id': 10037290024}, #10037290024 venda 15103342078 transferência
                                'loja': {'id': 205362589}
                            })

    return response


def obter_produto(numero_do_produto: int):
    access_token = get_bling_access_token()

    resposta = requests.get(
        url=f'{url_padrao}/produtos/{numero_do_produto}',
        headers={
            'Authorization': f'Bearer {access_token}'
        }
    )
    print(f'Requisição concluída para o produto de id {numero_do_produto}.')
    if resposta.status_code != 200:
        print(f"[Erro] Produto {numero_do_produto}: Status {resposta.status_code}")
        print(resposta.text)
        return None
    try:
        return resposta.json()
    except ValueError:
        print(f"[Erro] Produto {numero_do_produto}: resposta não é JSON")
        print(resposta.text)
        return None


def ler_situacoes():
    access_token = get_bling_access_token()
    resposta = requests.get(url=f'{url_padrao}/situacoes',
                            headers={
                                'Authorization': f'Bearer {access_token}'
                            }).json()
    return resposta


if __name__ == '__main__':
    #ler_nota_fiscal(22900407270)
    '''alterar_nota_fiscal(23421513475,
                        '19-miGGqp-kjINZeTd0ZClZFfxuCSBbyXgbb9ORW9be4',
                        378688497)'''
    #ler_planilha('19-miGGqp-kjINZeTd0ZClZFfxuCSBbyXgbb9ORW9be4',378688497)
    #pedidos_gerais()
    obter_pedido(23390571318)
    #obter_produto(15813948283)
    #get_bling_access_token()
    #ler_situacoes()
    #produtos_gerais()
