import extratores
import pandas as pd
import time


def conta_unica(conta, razao_social, apikey):
    categoria = extratores.tiny.conta_a_receber(conta['conta']['id'], apikey)
    dados_da_conta = {
        'RAZAO_SOCIAL': razao_social,
        'ID': conta['conta']['id'],
        'CLIENTE': conta['conta']['nome_cliente'],
        'HISTORICO': conta['conta']['historico'],
        'CATEGORIA': categoria['categoria'],
        'EMISSÃO': conta['conta']['data_emissao'],
        'VENCIMENTO': conta['conta']['data_vencimento'],
        'VALOR': float(conta['conta']['valor']),
        'SALDO': float(conta['conta']['saldo']),
        'SITUAÇÃO': conta['conta']['situacao']
    }

    return dados_da_conta


def multiplas_contas(apikey):
    dados = extratores.tiny.contas_a_receber(apikey)
    dados_contas, razao_social, apikey = dados
    contas = []
    i = 1
    for conta in dados_contas:
        contas.append(conta_unica(conta, razao_social, apikey))
        print(f'Conta {i} adicionada ao DataFrame.')
        i += 1
        time.sleep(3)

        if i == 50:
            break

    return contas


def multiplas_razoes_sociais(apis:list):
    todas_as_contas = []
    for api in apis:
        tabela_de_contas = multiplas_contas(api)
        todas_as_contas.extend(tabela_de_contas)

    contas_df = pd.DataFrame(todas_as_contas)

    return contas_df


if __name__ == '__main__':
    apikey1 = 'd2713e9ab254bee05f94f7d72a2cdc23d2df71e0370aefde2fd7b0b8b1c06338'
    apikey2 = 'be5001dee4cad6ed552b50096dd022b132bf209417c5469b8360f395272cd74c'
    apikey3 = '6f646fa987f2b6fa2bb8fd50024eba933c6befc0c0d37b395388b4107f34440b'
    apikey4 = '882086a25329f3c81061baa3159f521df591d629aa4a57651b87f6ab180dd6b4'
    apis = [apikey1, apikey2, apikey3, apikey4]
    #multiplas_contas(apikey1)
    multiplas_razoes_sociais(apis)
