import extratores
import carregadores
from pathlib import Path
import pandas as pd
import time
import re
from datetime import datetime


CACHE = Path(__file__).parent.parent / 'cache'


# numero, data, dataSaida, totalvenda, totalprodutos, desconto, valorfrete, custoprodutos, situacao, loja
def unicov2(pedido):
    dados_do_pedido = {
        "id_pedido": pedido["pedido"]["numero"],
        "data": pedido["pedido"]["data"],
        "mes": pedido["pedido"]["data"][:7],
        "data_saida": pedido["pedido"].get("dataSaida", None),
        "receita": float(pedido["pedido"]["totalvenda"]),
        "valor_dos_produtos": float(pedido["pedido"]["totalprodutos"]),
        "desconto": float(pedido["pedido"]["desconto"].replace(',','.')),
        "frete": float(pedido["pedido"]["valorfrete"]),
        "custo_produtos": sum(float(item["item"]["precocusto"]) if "precocusto" in item["item"].keys() and
                                                            item["item"]["precocusto"] is not None else 0
                              for item in pedido["pedido"]["itens"]),
        "situacao": pedido["pedido"]["situacao"],
        "loja": pedido["pedido"].get("loja", "nenhuma"),
    }

    return dados_do_pedido


def multiplosv2(dados):
    pedidos = []
    i = 1
    for pedido in dados:
        pedidos.append(unicov2(pedido))
        print(f'Produto {i} adicionado ao dataframe.')
        i += 1

    produtos_df = pd.DataFrame(pedidos)

    return produtos_df


def agrupando_por_canal(dataframe: pd.DataFrame):
    nomes_das_lojas = {
        '204408488': 'Amazon FBA Classic - Charme do Detalhe',
        '203966024': 'Amazon FBA Onsite - Charme do Detalhe',
        '204036478': 'Amazon DBA - Charme do Detalhe',
        '203660959': 'Yampi - Filial',
        '204130790': 'Magazine Luiza',
        '204038403': 'Mercado Livre - Super Criativo',
        '204037946': 'Mercado Livre - Mania de Lar',
        '204177738': 'Mercado Livre - Charme do Detalhe Full',
        '204021809': 'Mercado Livre - Charme do Detalhe',
        '204765312': 'Shein',
        '204347260': 'Shopee - Mania de Lar',
        '204021184': 'Shopee - Super Criativo',
        '204021181': 'Shopee - Charme do Detalhe'
    }

    dataframe['loja'] = dataframe['loja'].replace(nomes_das_lojas)

    df_agrupado = dataframe.groupby(['mes','loja']).agg({'receita': 'sum',
                                                 'valor_dos_produtos': 'sum',
                                                 'desconto': 'sum',
                                                 'frete': 'sum',
                                                 'custo_produtos': 'sum'}).reset_index()

    return df_agrupado


def situacoes(situacao_id):
    situacoes_dict = {
        24: 'Verificado',
        9: 'Atendido',
        6: 'Em aberto',
        12: 'Cancelado',
        15: 'Em andamento',
        130276: 'Em troca',
        118912: 'China Comunicado Atendimento',
        464030: 'Verificado Full',
        141129: 'Devolução',
        54829: 'CHARME DO DETALHE - DROPSHIPPING'
    }
    return situacoes_dict.get(situacao_id, f"Situação desconhecida: {situacao_id}")


def pedido_unico(pedido):
    lista_pedido = []
    nomes_das_lojas = {
        204408488: 'Amazon FBA Classic - Charme do Detalhe',
        203966024: 'Amazon FBA Onsite - Charme do Detalhe',
        204036478: 'Amazon DBA - Charme do Detalhe',
        203660959: 'Yampi - Filial',
        204130790: 'Magazine Luiza',
        204038403: 'Mercado Livre - Super Criativo',
        205092745: 'Mercado Livre - Super Criativo Full',
        204037946: 'Mercado Livre - Mania de Lar',
        204177738: 'Mercado Livre - Charme do Detalhe Full',
        204021809: 'Mercado Livre - Charme do Detalhe',
        204765312: 'Shein',
        204347260: 'Shopee - Mania de Lar',
        205428219: 'Shopee - Mania de Lar Full',
        204021184: 'Shopee - Super Criativo',
        204021181: 'Shopee - Charme do Detalhe',
        205324745: 'Mercado Livre - Mania de Lar Full',
        204996994: 'Loja Virtual Shopify 11:39:44',
        205007896: 'Charme do Detalhe - SITE',
        205429935: 'TikTok Shop',
        0: 'Nenhuma'
    }

    dados_do_pedido = {
        'id': pedido['id'],
        'numero': pedido['numero'],
        'data': pedido['data'],
        'numeroLoja': pedido['numeroLoja'],
        'loja_id': pedido['loja']['id'],
        'loja': nomes_das_lojas.get(pedido['loja']['id'], f'Loja desconhecida'),
        'cpf_cliente':
            int(re.sub(r'\D', '', pedido['contato'].get('numeroDocumento', '')))
            if re.sub(r'\D', '', pedido['contato'].get('numeroDocumento', ''))
            else 0,
        'nome_cliente': pedido['contato']['nome'],
        'situação': situacoes(pedido['situacao']['id']),
        'valor_dos_produtos': pedido['totalProdutos'],
        'valor_do_pedido': pedido['total'],
        'data_de_atualizacao': datetime.now().replace(microsecond=0)
    }

    detalhes_do_pedido = extratores.blingv3.obter_pedido(dados_do_pedido['id'])
    itens = detalhes_do_pedido['data']['itens']
    print(f'O pedido atual possui {len(itens)} itens.')
    campos_detalhados = {
        'observações': detalhes_do_pedido['data']['observacoes'],
        'observações_internas': detalhes_do_pedido['data']['observacoesInternas'],
        'desconto': detalhes_do_pedido['data']['desconto']['valor'],
        #'itens': detalhes_do_pedido['data']['itens'],
        'observações_de_pagamento':
            detalhes_do_pedido['data']['parcelas'][0]['observacoes']
            if detalhes_do_pedido['data']['parcelas']
            else '',
        'forma_de_pagamento':
            detalhes_do_pedido['data']['parcelas'][0]['formaPagamento']['id']
            if detalhes_do_pedido['data']['parcelas']
            else None,
        'frete': detalhes_do_pedido['data']['transporte']['frete'],
        'rua': detalhes_do_pedido['data']['transporte']['etiqueta']['endereco'],
        'rua_numero': detalhes_do_pedido['data']['transporte']['etiqueta']['numero'],
        'complemento': detalhes_do_pedido['data']['transporte']['etiqueta']['complemento'],
        'cidade': detalhes_do_pedido['data']['transporte']['etiqueta']['municipio'],
        'bairro': detalhes_do_pedido['data']['transporte']['etiqueta']['bairro'],
        'cep': detalhes_do_pedido['data']['transporte']['etiqueta']['cep'],
        'uf': detalhes_do_pedido['data']['transporte']['etiqueta']['uf']
    }

    pedido_completo = dados_do_pedido | campos_detalhados

    for item in itens:
        id_item = item['produto']['id']
        if id_item == 0:
            campos_do_item = {
                'codigo_item': None,
                'descricao_item': None,
                'quantidade': None,
                'unidade': None,
                'valor_item': None,
                'kit': None,
                'codigo_componente': None,
                'descricao_componente': None,
                'quantidade_componente': None,
                'unidade_componente': None,
                'valor_componente': None,
                'check_componente_kit': None
            }
            lista_pedido.append(pedido_completo | campos_do_item)
            continue
        detalhe_item = extratores.blingv3.obter_produto(id_item)
        estrutura_do_item = detalhe_item['data']['estrutura']['componentes']
        if len(estrutura_do_item) == 0:
            print(f"O item atual {item['descricao']} não possui componentes.")
            campos_do_item = {
                'codigo_item': item['codigo'],
                'descricao_item': item['descricao'],
                'quantidade': item['quantidade'],
                'unidade': item['unidade'],
                'valor_item': item['valor'],
                'kit': 'Não',
                'codigo_componente': None,
                'descricao_componente': None,
                'quantidade_componente': None,
                'unidade_componente': None,
                'valor_componente': None,
                'check_componente_kit': None
            }
            lista_pedido.append(pedido_completo | campos_do_item)
        if len(estrutura_do_item) != 0:
            print(f"O item atual {item['descricao']} possui {len(estrutura_do_item)} componente(s).")
            for componente in estrutura_do_item:
                time.sleep(0.5)
                quantidade_do_componente = componente['quantidade']
                detalhe_do_componente = extratores.blingv3.obter_produto(componente['produto']['id'])
                campos_do_item = {
                    'codigo_item': item['codigo'],
                    'descricao_item': item['descricao'],
                    'quantidade': item['quantidade'],
                    'unidade': item['unidade'],
                    'valor_item': item['valor'],
                    'kit': 'Sim',
                    'codigo_componente': detalhe_do_componente['data']['codigo'],
                    'descricao_componente': detalhe_do_componente['data']['nome'],
                    'quantidade_componente': quantidade_do_componente,
                    'unidade_componente': detalhe_do_componente['data']['unidade'],
                    'valor_componente': detalhe_do_componente['data']['preco'],
                    'check_componente_kit':
                        'Sim' if len(detalhe_do_componente['data']['estrutura']['componentes']) > 0 else 'Não'
                }
                print(f"Componente {detalhe_do_componente['data']['nome']} do item {item['descricao']} lido.")
                lista_pedido.append(pedido_completo | campos_do_item)

    return lista_pedido


def pedido_unico_sem_componente(pedido):
    lista_pedido = []
    nomes_das_lojas = {
        204408488: 'Amazon FBA Classic - Charme do Detalhe',
        203966024: 'Amazon FBA Onsite - Charme do Detalhe',
        204036478: 'Amazon DBA - Charme do Detalhe',
        203660959: 'Yampi - Filial',
        204130790: 'Magazine Luiza',
        204038403: 'Mercado Livre - Super Criativo',
        205092745: 'Mercado Livre - Super Criativo Full',
        204037946: 'Mercado Livre - Mania de Lar',
        204177738: 'Mercado Livre - Charme do Detalhe Full',
        204021809: 'Mercado Livre - Charme do Detalhe',
        204765312: 'Shein',
        204347260: 'Shopee - Mania de Lar',
        205428219: 'Shopee - Mania de Lar Full',
        204021184: 'Shopee - Super Criativo',
        204021181: 'Shopee - Charme do Detalhe',
        205324745: 'Mercado Livre - Mania de Lar Full',
        204996994: 'Loja Virtual Shopify 11:39:44',
        205007896: 'Charme do Detalhe - SITE',
        205429935: 'TikTok Shop',
        0: 'Nenhuma'
    }

    dados_do_pedido = {
        'id': pedido['id'],
        'numero': pedido['numero'],
        'data': pedido['data'],
        'numeroLoja': pedido['numeroLoja'],
        'loja_id': pedido['loja']['id'],
        'loja': nomes_das_lojas.get(pedido['loja']['id'], f'Loja desconhecida'),
        'cpf_cliente':
            int(re.sub(r'\D', '', pedido['contato'].get('numeroDocumento', '')))
            if re.sub(r'\D', '', pedido['contato'].get('numeroDocumento', ''))
            else 0,
        'nome_cliente': pedido['contato']['nome'],
        'situação': situacoes(pedido['situacao']['id']),
        'valor_dos_produtos': pedido['totalProdutos'],
        'valor_do_pedido': pedido['total'],
        'data_de_atualizacao': datetime.now().replace(microsecond=0)
    }

    detalhes_do_pedido = extratores.blingv3.obter_pedido(dados_do_pedido['id'])
    itens = detalhes_do_pedido['data']['itens']
    print(f'O pedido atual possui {len(itens)} itens.')
    volumes = detalhes_do_pedido['data']['transporte'].get('volumes', [])
    if volumes:  # só entra se houver pelo menos um volume
        id_volume = volumes[0]['id']
        detalhes_do_transporte = extratores.blingv3.logistica_objeto(id_volume)
        campos_detalhados_de_transporte = {
            'id_rastreamento': id_volume,
            'descricao_rastreamento': detalhes_do_transporte['data']['rastreamento'].get('descricao'),
            'codigo_rastreamento': detalhes_do_transporte['data']['rastreamento'].get('codigo'),
            'att_rastreamento': detalhes_do_transporte['data']['rastreamento'].get('ultimaAlteracao')
        }
    else:
        # fallback para quando não há volumes
        campos_detalhados_de_transporte = {
            'id_rastreamento': None,
            'descricao_rastreamento': None,
            'codigo_rastreamento': None,
            'att_rastreamento': None
        }
    #detalhes_do_transporte = extratores.blingv3.logistica_objeto(detalhes_do_pedido['data']['transporte']['volumes'][0]['id'])
    campos_detalhados = {
        'observações': detalhes_do_pedido['data']['observacoes'],
        'observações_internas': detalhes_do_pedido['data']['observacoesInternas'],
        'desconto': detalhes_do_pedido['data']['desconto']['valor'],
        'observações_de_pagamento':
            detalhes_do_pedido['data']['parcelas'][0]['observacoes']
            if detalhes_do_pedido['data']['parcelas']
            else '',
        'forma_de_pagamento':
            detalhes_do_pedido['data']['parcelas'][0]['formaPagamento']['id']
            if detalhes_do_pedido['data']['parcelas']
            else None,
        'frete': detalhes_do_pedido['data']['transporte']['frete'],
        'rua': detalhes_do_pedido['data']['transporte']['etiqueta']['endereco'],
        'rua_numero': detalhes_do_pedido['data']['transporte']['etiqueta']['numero'],
        'complemento': detalhes_do_pedido['data']['transporte']['etiqueta']['complemento'],
        'cidade': detalhes_do_pedido['data']['transporte']['etiqueta']['municipio'],
        'bairro': detalhes_do_pedido['data']['transporte']['etiqueta']['bairro'],
        'cep': detalhes_do_pedido['data']['transporte']['etiqueta']['cep'],
        'uf': detalhes_do_pedido['data']['transporte']['etiqueta']['uf']
    }

    pedido_completo = dados_do_pedido | campos_detalhados
    pedido_completo = pedido_completo | campos_detalhados_de_transporte

    for item in itens:
        id_item = item['produto']['id']
        if id_item == 0:
            campos_do_item = {
                'codigo_item': None,
                'descricao_item': None,
                'quantidade': None,
                'unidade': None,
                'valor_item': None
            }
            lista_pedido.append(pedido_completo | campos_do_item)
        else:
            campos_do_item = {
                'codigo_item': item['codigo'],
                'descricao_item': item['descricao'],
                'quantidade': item['quantidade'],
                'unidade': item['unidade'],
                'valor_item': item['valor']
            }
            lista_pedido.append(pedido_completo | campos_do_item)

    return lista_pedido


def multiplos_pedidos(dados):
    print(f'Caminho: {CACHE}')

    '''# Tentar carregar pedidos já existentes
    try:
        pedidos_existentes = pd.read_parquet(CACHE / 'pedidos.parquet')
        print(f"{len(pedidos_existentes)} linhas carregadas.")
    except FileNotFoundError:
        pedidos_existentes = pd.DataFrame(columns=['id', 'data_de_atualizacao'])
        print("Nenhum arquivo de pedidos encontrado. Começando do zero.")'''

    pedidos_existentes = extratores.google_cloud_storage.ler_arquivo_no_gcs()

    # Normalizar datas para apenas o dia, se houver pedidos existentes
    if not pedidos_existentes.empty:
        pedidos_existentes['data_de_atualizacao'] = pd.to_datetime(
            pedidos_existentes['data_de_atualizacao']
        ).dt.date

    # Definir todas as colunas obrigatórias
    colunas_obrigatorias = [
        'id', 'numero', 'data', 'numeroLoja', 'loja_id', 'loja',
        'cpf_cliente', 'nome_cliente', 'situação',
        'valor_dos_produtos', 'valor_do_pedido', 'data_de_atualizacao',
        'observações', 'observações_internas', 'desconto',
        'observações_de_pagamento', 'forma_de_pagamento', 'frete',
        'rua', 'rua_numero', 'complemento', 'cidade', 'bairro', 'cep', 'uf',
        'id_rastreamento', 'descricao_rastreamento', 'codigo_rastreamento', 'att_rastreamento',
        'codigo_item', 'descricao_item', 'quantidade', 'unidade', 'valor_item'
    ]

    for col in colunas_obrigatorias:
        if col not in pedidos_existentes.columns:
            pedidos_existentes[col] = None

    novos_pedidos = []
    checkpoint = 500
    cols_int = ['id_rastreamento', 'numero', 'cpf_cliente']

    for i, pedido in enumerate(dados, 1):
        #print(f"Executando pedido {i} de {len(dados)}, de número {pedido['numero']}")

        try:
            # Checar se o pedido já existe e comparar data
            id_pedido = pedido['id']
            data_atualizacao = datetime.now().date()  # até o dia

            filtro_existente = pedidos_existentes[pedidos_existentes['id'] == id_pedido]
            if filtro_existente.empty:
                print(f"Executando pedido {i} de {len(dados)}, de número {pedido['numero']}\n"
                      f"Pedido {id_pedido} é novo, será adicionado.")
                pedidos_do_pedido = pedido_unico_sem_componente(pedido)
                novos_pedidos.extend(pedidos_do_pedido)
                time.sleep(0.1)
            else:
                data_existente = pd.to_datetime(filtro_existente['data_de_atualizacao'].iloc[0]).date()
                #valores atuais do pedido pela API
                numero = pedido['numero']
                total_produtos = pedido['totalProdutos']
                total = pedido['total']
                situacao = situacoes(pedido['situacao']['id'])
                cpf_cliente = (
                    int(re.sub(r'\D', '', pedido['contato'].get('numeroDocumento', '')))) \
                    if re.sub(r'\D', '', pedido['contato'].get('numeroDocumento', '')) \
                    else 0
                pedido_existente = filtro_existente.iloc[0]

                diferenças = []
                if pedido_existente['numero'] != numero:
                    diferenças.append(f"numero: {pedido_existente['numero']} → {numero}")
                if pedido_existente['valor_dos_produtos'] != total_produtos:
                    diferenças.append(f"valor_dos_produtos: {pedido_existente['valor_dos_produtos']} → {total_produtos}")
                if pedido_existente['valor_do_pedido'] != total:
                    diferenças.append(f"valor_do_pedido: {pedido_existente['valor_do_pedido']} → {total}")
                if pedido_existente['situação'] != situacao:
                    diferenças.append(f"situação: {pedido_existente['situação']} → {situacao}")
                if pedido_existente['cpf_cliente'] != cpf_cliente:
                    diferenças.append(f"cpf_cliente: {pedido_existente['cpf_cliente']} → {cpf_cliente}")

                if diferenças:
                    print(f"Executando pedido {i} de {len(dados)}, de número {pedido['numero']}\n"
                          f"Pedido {id_pedido} foi atualizado. Diferenças detectadas:")
                    for diff in diferenças:
                        print(f"  - {diff}")
                    pedidos_existentes = pedidos_existentes[pedidos_existentes['id'] != id_pedido]
                    pedidos_do_pedido = pedido_unico_sem_componente(pedido)
                    novos_pedidos.extend(pedidos_do_pedido)
                    time.sleep(0.1)
                '''else:
                    print(f"Pedido {id_pedido} já existe e não foi atualizado, será ignorado.")'''
            if i % checkpoint == 0:
                print(f"Checkpoint atingido após {i} pedidos. Salvando progresso...")
                novos_pedidos_df = pd.DataFrame(novos_pedidos)
                pedidos_finais_parcial = pd.concat([pedidos_existentes, novos_pedidos_df], ignore_index=True)
                for col in cols_int:
                    if col in pedidos_finais_parcial.columns:  # garante que a coluna existe
                        pedidos_finais_parcial[col] = pedidos_finais_parcial[col].astype("Int64")
                carregadores.google_cloud_storage.salvando_no_gcs(novos_pedidos_df, pedidos_finais_parcial)
                carregadores.google_cloud_storage.salvando_no_bigquery(pedidos_finais_parcial)
                pedidos_finais_parcial.to_parquet(CACHE / 'pedidos.parquet', index=False, engine='pyarrow')
                #pedidos_finais_parcial.to_excel(CACHE / 'pedidos.xlsx', index=False)
                print("Checkpoint salvo com sucesso.")
        except Exception as e:
            print(f"Erro ao processar o pedido {pedido.get('id', 'sem id')}: {str(e)}")
            print("Salvando progresso parcial antes de interromper.")
            novos_pedidos_df = pd.DataFrame(novos_pedidos)
            pedidos_finais_parcial = pd.concat([pedidos_existentes, novos_pedidos_df], ignore_index=True)
            for col in cols_int:
                if col in pedidos_finais_parcial.columns:  # garante que a coluna existe
                    pedidos_finais_parcial[col] = pedidos_finais_parcial[col].astype("Int64")
            carregadores.google_cloud_storage.salvando_no_gcs(novos_pedidos_df, pedidos_finais_parcial)
            carregadores.google_cloud_storage.salvando_no_bigquery(pedidos_finais_parcial)
            pedidos_finais_parcial.to_parquet(CACHE / 'pedidos.parquet', index=False, engine='pyarrow')
            #pedidos_finais_parcial.to_excel(CACHE / 'pedidos.xlsx', index=False)
            raise  # relevanta a exceção depois de salvar

    # Concatenar os novos pedidos com os existentes
    novos_pedidos_df = pd.DataFrame(novos_pedidos)
    pedidos_finais = pd.concat([pedidos_existentes, novos_pedidos_df], ignore_index=True)
    for col in cols_int:
        if col in pedidos_finais.columns:  # garante que a coluna existe
            pedidos_finais[col] = pedidos_finais[col].astype("Int64")

    #pedidos_finais.to_excel(CACHE/'pedidos.xlsx', index=False)
    pedidos_finais.to_parquet(CACHE/'pedidos.parquet', index=False, engine='pyarrow')

    return novos_pedidos_df, pedidos_finais


if __name__ == '__main__':
    #base = extratores.bling.todos_os_pedidos()
    #agrupando_por_canal(multiplosv2(base))
    #pedido_unico(extratores.blingv3.obter_pedido(23225170135))
    multiplos_pedidos(extratores.blingv3.pedidos_gerais())
