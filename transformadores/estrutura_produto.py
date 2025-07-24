import extratores

import pandas as pd


def unico(produto):
    lista_produto = []
    grupos = {
        251080: 'PRODUTOS REVENDA',
        250624: 'FABRICAÇÃO PRÓPRIA',
        0: 'SEM GRUPO'
    }

    dados_do_produto = {
        "id": produto["id"],
        "codigo": produto["codigo"],
        "descricao": produto["nome"],
        "situacao": produto["situacao"],
        "preco": float(produto["preco"]),
        "custo": float(produto["precoCusto"]) if 'precoCusto' in produto.keys() else None
    }
    detalhes_do_produto = extratores.blingv3.obter_produto(produto['id'])
    componentes = detalhes_do_produto['data']['estrutura']['componentes']

    if len(componentes) == 0:
        campos_detalhados = {
            'grupo': grupos.get(detalhes_do_produto['data']['tributacao']['grupoProduto']['id'],
                                f'Grupo desconhecido: {detalhes_do_produto['data']['tributacao']['grupoProduto']['id']}'),
            'qtd_componentes': len(componentes),
            'id_componente': None,
            'qtd_componente': None
        }
        lista_produto.append(dados_do_produto | campos_detalhados)
    else:
        for componente in componentes:
            campos_detalhados = {
                'grupo': grupos.get(detalhes_do_produto['data']['tributacao']['grupoProduto']['id'],
                                    f'Grupo desconhecido: {detalhes_do_produto['data']['tributacao']['grupoProduto']['id']}'),
                'qtd_componentes': len(componentes),
                'id_componente': componente['produto']['id'],
                'qtd_componente': componente['quantidade']
            }
            lista_produto.append(dados_do_produto | campos_detalhados)

    return lista_produto


def multiplos(dados):
    produtos = []
    for i, produto in enumerate(dados, 1):
        produtos.extend(unico(produto))
        print(f'Produto {i} adicionado ao dataframe.')

    produtos_df = pd.DataFrame(produtos)

    return produtos_df


if __name__ == '__main__':
    multiplos(extratores.blingv3.produtos_gerais())
