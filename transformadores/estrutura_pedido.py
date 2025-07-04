import extratores

import pandas as pd


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


def pedido_unico(pedido):
    lista_pedido = []
    nomes_das_lojas = {
        204408488: 'Amazon FBA Classic - Charme do Detalhe',
        203966024: 'Amazon FBA Onsite - Charme do Detalhe',
        204036478: 'Amazon DBA - Charme do Detalhe',
        203660959: 'Yampi - Filial',
        204130790: 'Magazine Luiza',
        204038403: 'Mercado Livre - Super Criativo',
        204037946: 'Mercado Livre - Mania de Lar',
        204177738: 'Mercado Livre - Charme do Detalhe Full',
        204021809: 'Mercado Livre - Charme do Detalhe',
        204765312: 'Shein',
        204347260: 'Shopee - Mania de Lar',
        205428219: 'Shopee - Mania de Lar Full',
        204021184: 'Shopee - Super Criativo',
        204021181: 'Shopee - Charme do Detalhe',
        205324745: 'Mercado Livre - Mania de Lar Full',
        204996994: 'Loja Virtual Shopify 11:39:44'
    }
    situacoes = {
        24: 'Verificado',
        9: 'Atendido',
        6: 'Em aberto',
        12: 'Cancelado',
        15: 'Em andamento',
        130276: 'Em troca',
        118912: 'China Comunicado Atendimento',
        464030: 'Verificado Full'
    }
    dados_do_pedido = {
        'id': pedido['id'],
        'numero': pedido['numero'],
        'data': pedido['data'],
        'numeroLoja': pedido['numeroLoja'],
        'loja_id': pedido['loja']['id'],
        'loja': nomes_das_lojas.get(pedido['loja']['id'], f'Loja desconhecida'),
        'cpf_cliente': pedido['contato']['numeroDocumento'],
        'nome_cliente': pedido['contato']['nome'],
        'situação': situacoes.get(pedido['situacao']['id'], f"Situação desconhecida: {pedido['situacao']['id']}"),
        'valor_dos_produtos': pedido['totalProdutos'],
        'valor_do_pedido': pedido['total']
    }

    detalhes_do_pedido = extratores.blingv3.obter_pedido(dados_do_pedido['id'])
    itens = detalhes_do_pedido['data']['itens']
    campos_detalhados = {
        'observações': detalhes_do_pedido['data']['observacoes'],
        'observações_internas': detalhes_do_pedido['data']['observacoesInternas'],
        'desconto': detalhes_do_pedido['data']['desconto']['valor'],
        #'itens': detalhes_do_pedido['data']['itens'],
        'observações_de_pagamento': detalhes_do_pedido['data']['parcelas'][0]['observacoes'],
        'forma_de_pagamento': detalhes_do_pedido['data']['parcelas'][0]['formaPagamento']['id'],
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
        detalhe_item = {
            'codigo_item': item['codigo'],
            'descricao_item': item['descricao'],
            'quantidade': item['quantidade'],
            'unidade': item['unidade'],
            'valor_item': item['valor']
        }
        lista_pedido.append(pedido_completo | detalhe_item)

    return lista_pedido


def multiplos_pedidos(dados):
    pedidos = []

    for i, pedido in enumerate(dados, 1):
        print(f'Executando pedido {i} de {len(dados)}')
        pedidos.append(pedido_unico(pedido))

    pedidos_lista = [item for sublista in pedidos for item in sublista]

    pedidos_df = pd.DataFrame(pedidos_lista)

    return pedidos_df


if __name__ == '__main__':
    #base = extratores.bling.todos_os_pedidos()
    #agrupando_por_canal(multiplosv2(base))
    #pedido_unico(extratores.blingv3.obter_pedido(23225170135))
    multiplos_pedidos(extratores.blingv3.pedidos_gerais())
