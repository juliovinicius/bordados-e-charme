import extratores


def unico(produto):
    dados_do_produto = {
        "id_produto": produto["produto"]["id"],
        "codigo": produto["produto"]["codigo"],
        "descricao": produto["produto"]["descricao"],
        "situacao": produto["produto"]["situacao"],
        "unidade": produto["produto"].get("unidade", None),
        "preco": produto["produto"]["preco"],
        "custo": produto["produto"]["precoCusto"],
        "fornecedor": produto["produto"].get("fornecedor", None),
        "observacoes": produto["produto"]["observacoes"],
        "grupo_produto": produto["produto"]["grupoProduto"]
    }

    return dados_do_produto


if __name__ == '__main__':
    base = extratores.bling.todos_os_produtos()
    for produto in base:
        print(unico(produto), "\n\n")
