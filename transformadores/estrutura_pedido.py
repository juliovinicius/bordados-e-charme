import extratores

import pandas as pd


# numero, data, dataSaida, totalvenda, totalprodutos, desconto, valorfrete, custoprodutos, situacao, loja
def unico(pedido):
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


def multiplos(dados):
    pedidos = []
    i = 1
    for pedido in dados:
        pedidos.append(unico(pedido))
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


if __name__ == '__main__':
    base = extratores.bling.todos_os_pedidos()
    agrupando_por_canal(multiplos(base))
