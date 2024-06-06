import extratores

import pandas as pd


def unico(produto):
    dados_do_produto = {
        "id_produto": produto["produto"]["id"],
        "codigo": produto["produto"]["codigo"],
        "descricao": produto["produto"]["descricao"],
        "situacao": produto["produto"]["situacao"],
        "unidade": produto["produto"].get("unidade", None),
        "preco": float(produto["produto"]["preco"]),
        "custo": float(produto["produto"]["precoCusto"]) if 'precoCusto' in produto['produto'].keys()
                                                           and produto['produto']['precoCusto'] is not None else 'vazio',
        "fornecedor": produto["produto"].get("fornecedor", None),
        "data_inclusao": produto["produto"]["dataInclusao"],
        "data_alteracao": produto["produto"].get("dataAlteracao", "sem alteração"),
        "observacoes": produto["produto"]["observacoes"],
        "grupo_produto": produto["produto"]["grupoProduto"]
    }

    return dados_do_produto


def multiplos(dados):
    produtos = []
    i = 1
    for produto in dados:
        produtos.append(unico(produto))
        print(f'Produto {i} adicionado ao dataframe.')
        i += 1

    produtos_df = pd.DataFrame(produtos)

    return produtos_df


if __name__ == '__main__':
    base = extratores.bling.todos_os_produtos()
    print(multiplos(base))
