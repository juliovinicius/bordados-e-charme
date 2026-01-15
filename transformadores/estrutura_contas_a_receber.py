import extratores
import pandas as pd
import time
from datetime import date
from pathlib import Path
import fastparquet
from extratores.tiny import data_ini_emissao, data_fim_emissao


CAMINHO_ARQUIVO_PARQUET = Path(__file__).parent.parent / 'cache' / 'contas.parquet'


def conta_unica(conta, razao_social, apikey, tipo, backup):
    categoria = (extratores.tiny.conta_a_receber(conta['conta']['id'], apikey)
                 if tipo == 'receber'
                 else extratores.tiny.conta_a_pagar(conta['conta']['id'], apikey))
    dados_da_conta = {
        'RAZAO_SOCIAL': razao_social,
        'ID': conta['conta']['id'],
        'TIPO': tipo,
        'NRO_DOCUMENTO': conta['conta']['numero_doc'],
        'CLIENTE': conta['conta']['nome_cliente'],
        'HISTORICO': conta['conta']['historico'],
        'CATEGORIA': categoria['categoria'],
        'EMISSÃO': pd.to_datetime(conta['conta']['data_emissao'], format='%d/%m/%Y'),
        'VENCIMENTO': pd.to_datetime(conta['conta']['data_vencimento'], format='%d/%m/%Y'),
        'LIQUIDAÇÃO': pd.to_datetime(categoria['liquidacao'], format='%d/%m/%Y') if 'liquidacao' in categoria else None,
        'VALOR': float(conta['conta']['valor']),
        'SALDO': float(conta['conta']['saldo']),
        'SITUAÇÃO': conta['conta']['situacao'],
        'DATA_LEITURA': pd.Timestamp(date.today()),
        'HISTÓRICO': '' if backup == 'não' else 'BACKUP'
    }

    return dados_da_conta


def multiplas_contas(apikey, caminho_parquet=CAMINHO_ARQUIVO_PARQUET):
    if caminho_parquet:
        try:
            parquet_existente = pd.read_parquet(caminho_parquet)
            print('Arquivo parquet encontrado.')
            registros_existentes = {(row['ID'],
                                     row['RAZAO_SOCIAL']): {
                'saldo': row['SALDO'],
                'data_leitura': row['DATA_LEITURA'],
                'vencimento': pd.to_datetime(row['VENCIMENTO'],format='%d/%m/%Y'),
                'backup': 'sim' if row['HISTÓRICO'] == 'BACKUP' else 'não'
            } for _, row in parquet_existente.iterrows()}
        except FileNotFoundError:
            print('Parquet de contas anteriores não encontrado, iniciando um novo.')
            parquet_existente = pd.DataFrame(columns=[
                'RAZAO_SOCIAL',
                'ID',
                'TIPO',
                'NRO_DOCUMENTO',
                'CLIENTE',
                'HISTORICO',
                'CATEGORIA',
                'EMISSÃO',
                'VENCIMENTO',
                'LIQUIDAÇÃO',
                'VALOR',
                'SALDO',
                'SITUAÇÃO',
                'DATA_LEITURA',
                'HISTÓRICO'])
            registros_existentes = {}
    else:
        print('Caminho parquet não existe.')
        parquet_existente = pd.DataFrame(columns=[
            'RAZAO_SOCIAL',
            'ID',
            'TIPO',
            'NRO_DOCUMENTO',
            'CLIENTE',
            'HISTORICO',
            'CATEGORIA',
            'EMISSÃO',
            'VENCIMENTO',
            'LIQUIDAÇÃO',
            'VALOR',
            'SALDO',
            'SITUAÇÃO',
            'DATA_LEITURA',
            'HISTÓRICO'
        ])
        registros_existentes = {}

    dados_recebimento = extratores.tiny.contas_a_receber(apikey, data_ini_emissao, data_fim_emissao)
    dados_contas_a_receber, razao_social, apikey, tipo = dados_recebimento

    contas = []
    limite = 601
    pausa = 3
    i, j = 1, 1
    data_referencia = pd.Timestamp(date.today().replace(day=1)) - pd.DateOffset(months=1)
    data_referencia_futura = pd.Timestamp(date.today().replace(day=1)) + pd.DateOffset(months=3)
    data_atual = pd.Timestamp(date.today())

    print('Iniciando leitura de contas a receber.')
    for conta in dados_contas_a_receber:
        chave = (conta['conta']['id'], razao_social)
        saldo = float(conta['conta']['saldo'])
        if chave in registros_existentes:
            if registros_existentes[chave]['saldo'] != saldo:
                print(f"Conta {chave} saldo atualizado de {registros_existentes[chave]['saldo']} para {saldo}. Substituindo.")
                registros_existentes[chave]['saldo'] = saldo
                contas = [c for c in contas if (c['ID'], c['RAZAO_SOCIAL']) != chave]
                contas.append(conta_unica(conta, razao_social, apikey, tipo, backup='não'))
                print(f'Conta {i} substituída no DataFrame.')
                i += 1
                time.sleep(pausa)
                if i == limite:
                    print('Limite de execução atingido.')
                    break
            if (registros_existentes[chave]['saldo'] == saldo
                    and registros_existentes[chave]['vencimento'] >= data_referencia
                    and registros_existentes[chave]['data_leitura'] != data_atual
                    and registros_existentes[chave]['vencimento'] < data_referencia_futura):
                #print(f'Conta {chave} já existe com o mesmo saldo. Ignorando.')
                print(f"Conta {chave} de vencimento {registros_existentes[chave]['vencimento']} já existe com o mesmo saldo. Inserindo/atualizando leitura.")
                contas = [c for c in contas if (c['ID'], c['RAZAO_SOCIAL']) != chave]
                contas.append(conta_unica(conta, razao_social, apikey, tipo, backup='não'))
                print(f'Conta {i} com leitura atualizada no DataFrame.')
                i += 1
                time.sleep(pausa)
                if i == limite:
                    print('Limite de execução atingido.')
                    break
            if (registros_existentes[chave]['saldo'] == saldo
                    and registros_existentes[chave]['vencimento'] < data_referencia
                    and registros_existentes[chave]['backup'] == 'não'):
                print(f"Conta {chave} de vencimento {registros_existentes[chave]['vencimento']} está sendo lida para backup.")
                contas = [c for c in contas if (c['ID'], c['RAZAO_SOCIAL']) != chave]
                contas.append(conta_unica(conta, razao_social, apikey, tipo, backup='sim'))
                print(f'Conta {i} com leitura atualizada no DataFrame.')
                i += 1
                time.sleep(pausa)
                if i == limite:
                    print('Limite de execução atingido.')
                    break
            '''else:
                print(f'Conta já atualizada')'''
        else:
            print(f'Conta {chave} é nova. Adicionando.')
            #registros_existentes[chave]['saldo'] = saldo
            contas.append(conta_unica(conta, razao_social, apikey, tipo, backup='não'))
            print(f'Conta {i} adicionada ao DataFrame.')
            i += 1
            time.sleep(pausa)
            if i == limite:
                print('Limite de execução atingido.')
                break

    dados_pagamento = extratores.tiny.contas_a_pagar(apikey, data_ini_emissao, data_fim_emissao)
    dados_contas_a_pagar, razao_social, apikey, tipo = dados_pagamento

    print('Iniciando leitura de contas a pagar.')
    for conta in dados_contas_a_pagar:
        chave = (conta['conta']['id'], razao_social)
        saldo = float(conta['conta']['saldo'])
        if chave in registros_existentes:
            if registros_existentes[chave]['saldo'] != saldo:
                print(f"Conta {chave} saldo atualizado de {registros_existentes[chave]['saldo']} para {saldo}. Substituindo.")
                registros_existentes[chave]['saldo'] = saldo
                contas = [c for c in contas if (c['ID'], c['RAZAO_SOCIAL']) != chave]
                contas.append(conta_unica(conta, razao_social, apikey, tipo, backup='não'))
                print(f'Conta {j} substituída no DataFrame.')
                j += 1
                time.sleep(pausa)
                if j == limite:
                    print('Limite de execução atingido.')
                    break
            if (registros_existentes[chave]['saldo'] == saldo
                    and registros_existentes[chave]['vencimento'] >= data_referencia
                    and registros_existentes[chave]['data_leitura'] != data_atual
                    and registros_existentes[chave]['vencimento'] < data_referencia_futura):
                #print(f'Conta {chave} já existe com o mesmo saldo. Ignorando.')
                print(f'Conta {chave} já existe com o mesmo saldo. Inserindo/atualizando leitura.')
                contas = [c for c in contas if (c['ID'], c['RAZAO_SOCIAL']) != chave]
                contas.append(conta_unica(conta, razao_social, apikey, tipo, backup='não'))
                print(f'Conta {j} com leitura atualizada no DataFrame.')
                j += 1
                time.sleep(pausa)
                if j == limite:
                    print('Limite de execução atingido.')
                    break
            if (registros_existentes[chave]['saldo'] == saldo
                    and registros_existentes[chave]['vencimento'] < data_referencia
                    and registros_existentes[chave]['backup'] == 'não'):
                #print(f'Conta {chave} já existe com o mesmo saldo. Ignorando.')
                print(f"Conta {chave} de vencimento {registros_existentes[chave]['vencimento']} está sendo lida para backup.")
                contas = [c for c in contas if (c['ID'], c['RAZAO_SOCIAL']) != chave]
                contas.append(conta_unica(conta, razao_social, apikey, tipo, backup='sim'))
                print(f'Conta {j} com leitura atualizada no DataFrame.')
                j += 1
                time.sleep(pausa)
                if j == limite:
                    print('Limite de execução atingido.')
                    break
            '''else:
                print('Conta já atualizada.')'''
        else:
            print(f'Conta {chave} é nova. Adicionando.')
            #registros_existentes[chave]['saldo'] = saldo
            contas.append(conta_unica(conta, razao_social, apikey, tipo, backup='não'))
            print(f'Conta {j} adicionada ao DataFrame.')
            j += 1
            time.sleep(pausa)
            if j == limite:
                print('Limite de execução atingido.')
                break

    if i < limite:
        print(f'Não há mais contas a receber. {i-1} contas foram adicionadas/atualizadas.')
    if j < limite:
        print(f'Não há mais contas a pagar. {j-1} contas foram adicionadas/atualizadas.')

    contas_novas = pd.DataFrame(contas,columns=[
        'RAZAO_SOCIAL',
        'ID',
        'TIPO',
        'NRO_DOCUMENTO',
        'CLIENTE',
        'HISTORICO',
        'CATEGORIA',
        'EMISSÃO',
        'VENCIMENTO',
        'LIQUIDAÇÃO',
        'VALOR',
        'SALDO',
        'SITUAÇÃO',
        'DATA_LEITURA',
        'HISTÓRICO'
    ])
    '''contas_totais = (pd.concat([parquet_existente, contas_novas], ignore_index=True).
                     drop_duplicates(subset=['ID', 'RAZAO_SOCIAL'], keep='last'))'''

    return contas_novas


def multiplas_razoes_sociais(apis:list, caminho_parquet=CAMINHO_ARQUIVO_PARQUET):
    if caminho_parquet:
        try:
            parquet_existente = pd.read_parquet(caminho_parquet)
            print('Arquivo parquet encontrado.')
        except FileNotFoundError:
            print('Parquet de contas anteriores não encontrado, iniciando um novo.')
            parquet_existente = pd.DataFrame(columns=[
                'RAZAO_SOCIAL',
                'ID',
                'TIPO',
                'NRO_DOCUMENTO',
                'CLIENTE',
                'HISTORICO',
                'CATEGORIA',
                'EMISSÃO',
                'VENCIMENTO',
                'LIQUIDAÇÃO',
                'VALOR',
                'SALDO',
                'SITUAÇÃO',
                'DATA_LEITURA',
                'HISTÓRICO'])
    else:
        print('Caminho parquet não existe.')
        parquet_existente = pd.DataFrame(columns=[
            'RAZAO_SOCIAL',
            'ID',
            'TIPO',
            'NRO_DOCUMENTO',
            'CLIENTE',
            'HISTORICO',
            'CATEGORIA',
            'EMISSÃO',
            'VENCIMENTO',
            'LIQUIDAÇÃO',
            'VALOR',
            'SALDO',
            'SITUAÇÃO',
            'DATA_LEITURA',
            'HISTÓRICO'
        ])
    todas_as_contas = pd.DataFrame(parquet_existente)
    i = 1
    for api in apis:
        print(f'\n\nEm contato com a API {i}.')
        tabela_de_contas = multiplas_contas(api, CAMINHO_ARQUIVO_PARQUET)
        todas_as_contas = (pd.concat([todas_as_contas, tabela_de_contas], ignore_index=True).
                           drop_duplicates(subset=['ID', 'RAZAO_SOCIAL'], keep='last'))
        i += 1

    todas_as_contas = todas_as_contas.sort_values(
        by=['RAZAO_SOCIAL', 'TIPO', 'LIQUIDAÇÃO', 'VENCIMENTO'],
        ascending=[True, True, False, False]
    )
    todas_as_contas.to_parquet(CAMINHO_ARQUIVO_PARQUET, index=False)
    print(f'Dataframe atualizado em {CAMINHO_ARQUIVO_PARQUET}.')

    return todas_as_contas


if __name__ == '__main__':
    apikey1 = 'd2713e9ab254bee05f94f7d72a2cdc23d2df71e0370aefde2fd7b0b8b1c06338'
    apikey2 = 'be5001dee4cad6ed552b50096dd022b132bf209417c5469b8360f395272cd74c'
    apikey3 = '6f646fa987f2b6fa2bb8fd50024eba933c6befc0c0d37b395388b4107f34440b'
    apikey4 = '882086a25329f3c81061baa3159f521df591d629aa4a57651b87f6ab180dd6b4'
    apikey5 = 'dede5c75ea14c62541f2896a93b323baac894cb5655957bb6770cae4037f7319'
    apikey6 = '3d4c4e3432ae1d59fdf6671d195efaee373f5d5f6105440bbfdaf49f52429299'
    apis = [apikey1, apikey3, apikey4, apikey5, apikey6, apikey2]
    #multiplas_contas(apikey2)
    multiplas_razoes_sociais(apis)
