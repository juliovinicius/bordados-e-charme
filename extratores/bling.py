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

    return produtos


if __name__ == '__main__':
    todos_os_produtos()
