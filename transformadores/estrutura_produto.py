import extratores

import pandas as pd
from pathlib import Path
from datetime import datetime


CACHE = Path(__file__).parent.parent / 'cache'


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
        'data_de_atualizacao': datetime.now().replace(microsecond=0),
        "preco": float(produto["preco"]),
        "custo": float(produto["precoCusto"]) if 'precoCusto' in produto.keys() else None
    }
    detalhes_do_produto = extratores.blingv3.obter_produto(produto['id'])
    if detalhes_do_produto is None:
        print(f"Produto {produto['id']} ignorado por erro na API.")
        return []
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
    try:
        produtos_existentes = pd.read_parquet(CACHE / 'produtos_v2.parquet')
        print(f"{len(produtos_existentes)} linhas carregadas.")
    except FileNotFoundError:
        produtos_existentes = pd.DataFrame(columns=[
            'id',
            'codigo',
            'descricao',
            'situacao',
            'data_de_atualizacao',
            'preco',
            'custo',
            'grupo',
            'qtd_componentes',
            'id_componente',
            'qtd_componente'
        ])
        print("Nenhum arquivo de produtos encontrado. Começando do zero.")

    novos_produtos = []
    checkpoint = 100
    for i, produto in enumerate(dados, 1):
        print(f'Executando produto {i} de {len(dados)}, de código {produto['codigo']}.')
        try:
            id_produto = produto['id']
            filtro = produtos_existentes[produtos_existentes['id'] == id_produto]
            if filtro.empty:
                print(f'Produto {id_produto} é novo. Adicionando.')
                infos_do_produto = unico(produto)
                novos_produtos.extend(infos_do_produto)
            else:
                produto_existente = filtro.iloc[0]
                diferenças = []
                if produto_existente['codigo'] != produto['codigo']:
                    diferenças.append(f'código: {produto_existente['codigo']} → {produto['codigo']}')
                if produto_existente['descricao'] != produto['nome']:
                    diferenças.append(f'nome: {produto_existente['descricao']} → {produto['nome']}')
                if produto_existente['preco'] != produto['preco']:
                    diferenças.append(f'preço: {produto_existente['preco']} → {produto['preco']}')
                if produto_existente['situacao'] != produto['situacao']:
                    diferenças.append(f'situação: {produto_existente['situacao']} → {produto['situacao']}')

                if diferenças:
                    print(f'Produto {id_produto} foi modificado. Diferenças detectadas:')
                    for diff in diferenças:
                        print(f' - {diff}')
                    produtos_existentes = produtos_existentes[produtos_existentes['id'] != id_produto]
                    infos_do_produto = unico(produto)
                    novos_produtos.extend(infos_do_produto)
                else:
                    print(f'Produto {id_produto} não foi atualizado. Seguindo.')
            if i % checkpoint == 0:
                print(f'Checkpoint atingido após {i} produtos. Salvando progresso.')
                novos_produtos_df = pd.DataFrame(novos_produtos)
                parcial_de_produtos = pd.concat([produtos_existentes, novos_produtos_df], ignore_index=True)
                parcial_de_produtos.to_parquet(CACHE/'produtos_v2.parquet', index=False)
                print('Checkpoint salvo com sucesso.')
        except Exception as e:
            print(f'Erro ao processar o produto {produto['id']}: {str(e)}\n'
                  f'Salvando progresso parcial antes de interromper.')
            novos_produtos_df = pd.DataFrame(novos_produtos)
            parcial_de_produtos = pd.concat([produtos_existentes, novos_produtos_df], ignore_index=True)
            parcial_de_produtos.to_parquet(CACHE/'produtos_v2.parquet', index=False)
            raise

    novos_produtos_df = pd.DataFrame(novos_produtos)
    produtos_finais = pd.concat([produtos_existentes, novos_produtos_df], ignore_index=True)
    produtos_finais.to_parquet(CACHE/'produtos_v2.parquet', index=False)

    return produtos_finais


if __name__ == '__main__':
    multiplos(extratores.blingv3.produtos_gerais())
