import extratores
import pandas as pd
import time
from pathlib import Path
import fastparquet


CAMINHO_ARQUIVO_PARQUET = Path(__file__).parent.parent / 'cache' / 'contas.parquet'


def conta_unica(conta, razao_social, apikey, tipo):
    categoria = (extratores.tiny.conta_a_receber(conta['conta']['id'], apikey)
                 if tipo == 'receber'
                 else extratores.tiny.conta_a_pagar(conta['conta']['id'], apikey))
    dados_da_conta = {
        'RAZAO_SOCIAL': razao_social,
        'ID': conta['conta']['id'],
        'TIPO': tipo,
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


def multiplas_contas(apikey, caminho_parquet):
    if caminho_parquet:
        try:
            parquet_existente = pd.read_parquet(caminho_parquet)
            print('Arquivo parquet encontrado.')
            registros_existentes = {(row['ID'],
                                     row['RAZAO_SOCIAL']):row['SALDO'] for _, row in parquet_existente.iterrows()}
        except FileNotFoundError:
            print('Parquet de contas anteriores não encontrado, iniciando um novo.')
            parquet_existente = pd.DataFrame(columns=[
                'RAZAO_SOCIAL',
                'ID',
                'TIPO',
                'CLIENTE',
                'HISTORICO',
                'CATEGORIA',
                'EMISSÃO',
                'VENCIMENTO',
                'VALOR',
                'SALDO',
                'SITUAÇÃO'])
            registros_existentes = {}
    else:
        print('Caminho parquet não existe.')
        parquet_existente = pd.DataFrame(columns=[
            'RAZAO_SOCIAL',
            'ID',
            'TIPO',
            'CLIENTE',
            'HISTORICO',
            'CATEGORIA',
            'EMISSÃO',
            'VENCIMENTO',
            'VALOR',
            'SALDO',
            'SITUAÇÃO'
        ])
        registros_existentes = {}

    dados_recebimento = extratores.tiny.contas_a_receber(apikey)
    dados_contas_a_receber, razao_social, apikey, tipo = dados_recebimento

    contas = []
    limite = 10
    pausa = 5
    i, j = 1, 1

    print('Iniciando leitura de contas a receber.')
    for conta in dados_contas_a_receber:
        chave = (conta['conta']['id'], razao_social)
        saldo = float(conta['conta']['saldo'])
        if chave in registros_existentes:
            if registros_existentes[chave] != saldo:
                print(f'Conta {chave} saldo atualizado de {registros_existentes[chave]} para {saldo}. Substituindo.')
                registros_existentes[chave] = saldo
                contas = [c for c in contas if (c['id'], c['razao_social']) != chave]
                contas.append(conta_unica(conta, razao_social, apikey, tipo))
                print(f'Conta {i} substituída no DataFrame.')
                i += 1
                time.sleep(pausa)
                if i == limite:
                    print('Limite de execução atingido.')
                    break
            else:
                print(f'Conta {chave} já existe com o mesmo saldo. Ignorando.')
        else:
            print(f'Conta {chave} é nova. Adicionando.')
            registros_existentes[chave] = saldo
            contas.append(conta_unica(conta, razao_social, apikey, tipo))
            print(f'Conta {i} adicionada ao DataFrame.')
            i += 1
            time.sleep(pausa)
            if i == limite:
                print('Limite de execução atingido.')
                break

    dados_pagamento = extratores.tiny.contas_a_pagar(apikey)
    dados_contas_a_pagar, razao_social, apikey, tipo = dados_pagamento

    print('Iniciando leitura de contas a pagar.')
    for conta in dados_contas_a_pagar:
        chave = (conta['conta']['id'], razao_social)
        saldo = float(conta['conta']['saldo'])
        if chave in registros_existentes:
            if registros_existentes[chave] != saldo:
                print(f'Conta {chave} saldo atualizado de {registros_existentes[chave]} para {saldo}. Substituindo.')
                registros_existentes[chave] = saldo
                contas = [c for c in contas if (c['id'], c['razao_social']) != chave]
                contas.append(conta_unica(conta, razao_social, apikey, tipo))
                print(f'Conta {j} substituída no DataFrame.')
                j += 1
                time.sleep(pausa)
                if j == limite:
                    print('Limite de execução atingido.')
                    break
            else:
                print(f'Conta {chave} já existe com o mesmo saldo. Ignorando.')
        else:
            print(f'Conta {chave} é nova. Adicionando.')
            registros_existentes[chave] = saldo
            contas.append(conta_unica(conta, razao_social, apikey, tipo))
            print(f'Conta {j} adicionada ao DataFrame.')
            j += 1
            time.sleep(pausa)
            if j == limite:
                print('Limite de execução atingido.')
                break

    if i < limite:
        print('Não há mais contas a receber.')
    if j < limite:
        print('Não há mais contas a pagar.')

    contas_novas = pd.DataFrame(contas)
    contas_totais = (pd.concat([parquet_existente, contas_novas], ignore_index=True).
                     drop_duplicates(subset=['ID', 'RAZAO_SOCIAL'], keep='last'))

    return contas_totais


def multiplas_razoes_sociais(apis:list):
    todas_as_contas = pd.DataFrame()
    i = 1
    for api in apis:
        print(f'\n\nEm contato com a API {i}.')
        tabela_de_contas = multiplas_contas(api, CAMINHO_ARQUIVO_PARQUET)
        todas_as_contas = (pd.concat([todas_as_contas, tabela_de_contas], ignore_index=True).
                           drop_duplicates(subset=['ID', 'RAZAO_SOCIAL'], keep='last'))
        i += 1

    todas_as_contas.to_parquet(CAMINHO_ARQUIVO_PARQUET, index=False)
    print(f'Dataframe atualizado em {CAMINHO_ARQUIVO_PARQUET}.')

    return todas_as_contas


if __name__ == '__main__':
    apikey1 = 'd2713e9ab254bee05f94f7d72a2cdc23d2df71e0370aefde2fd7b0b8b1c06338'
    apikey2 = 'be5001dee4cad6ed552b50096dd022b132bf209417c5469b8360f395272cd74c'
    apikey3 = '6f646fa987f2b6fa2bb8fd50024eba933c6befc0c0d37b395388b4107f34440b'
    apikey4 = '882086a25329f3c81061baa3159f521df591d629aa4a57651b87f6ab180dd6b4'
    apis = [apikey1, apikey2, apikey3, apikey4]
    #multiplas_contas(apikey1)
    multiplas_razoes_sociais(apis)
