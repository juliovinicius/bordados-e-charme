from pathlib import Path
import pandas as pd


CAMINHO_CSVS = Path(__file__).parent /'csvs'


def juntando_csvs():
    csvs = list(CAMINHO_CSVS.glob('*.csv'))
    print(f'{len(csvs)} csvs encontrados.')
    print(f'{CAMINHO_CSVS}\n{CAMINHO_CSVS.parent}\n{CAMINHO_CSVS.parent.parent}')

    df_composto = []
    for csv in csvs:
        print(f'Lendo csv {csv.name}.')
        df = pd.read_csv(csv, sep=";")
        df_composto.append(df)

    df_final = pd.concat(df_composto, ignore_index=True)
    df_final['Localização'] = df_final['Localização'].astype(str)
    df_final['Código na Lista de Serviços'] = df_final['Código na Lista de Serviços'].astype(str)
    df_final['Grupo Tag'] = df_final['Grupo de Tags/Tags'].str.extract(r'Grupo:([^|]*)')
    df_final['Origem Tag'] = df_final['Grupo de Tags/Tags'].str.extract(r'ORIGEM:([^|]*)')
    df_final['Grupo Tag'] = df_final['Grupo Tag'].fillna('Desconhecido')
    df_final['Origem Tag'] = df_final['Origem Tag'].fillna('Desconhecido')

    novos_nomes = {
        'ID': 'id',
        'Código': 'codigo',
        'Descrição': 'descricao',
        'Unidade': 'unidade',
        'NCM': 'ncm',
        'Origem': 'origem',
        'Preço': 'preco',
        'Valor IPI fixo': 'valor_ipi_fixo',
        'Observações': 'observacoes',
        'Situação': 'situacao',
        'Estoque': 'estoque',
        'Preço de custo': 'preco_custo',
        'Cód. no fornecedor': 'codigo_fornecedor',
        'Fornecedor': 'fornecedor',
        'Localização': 'localizacao',
        'Estoque máximo': 'estoque_maximo',
        'Estoque mínimo': 'estoque_minimo',
        'Peso líquido (Kg)': 'peso_liquido_kg',
        'Peso bruto (Kg)': 'peso_bruto_kg',
        'GTIN/EAN': 'gtin_ean',
        'GTIN/EAN da Embalagem': 'gtin_ean_embalagem',
        'Largura do produto': 'largura',
        'Altura do Produto': 'altura',
        'Profundidade do produto': 'profundidade',
        'Data Validade': 'data_validade',
        'Descrição do Produto no Fornecedor': 'descricao_fornecedor',
        'Descrição Complementar': 'descricao_complementar',
        'Itens p/ caixa': 'itens_por_caixa',
        'Produto Variação': 'produto_variacao',
        'Tipo Produção': 'tipo_producao',
        'Classe de enquadramento do IPI': 'classe_ipi',
        'Código na Lista de Serviços': 'codigo_lista_servicos',
        'Tipo do item': 'tipo_item',
        'Grupo de Tags/Tags': 'tags',
        'Tributos': 'tributos',
        'Código Pai': 'codigo_pai',
        'Código Integração': 'codigo_integracao',
        'Grupo de produtos': 'grupo_produtos',
        'Marca': 'marca',
        'CEST': 'cest',
        'Volumes': 'volumes',
        'Descrição Curta': 'descricao_curta',
        'Cross-Docking': 'cross_docking',
        'URL Imagens Externas': 'url_imagens_externas',
        'Link Externo': 'link_externo',
        'Meses Garantia no Fornecedor': 'garantia_meses',
        'Clonar dados do pai': 'clonar_dados_pai',
        'Condição do Produto': 'condicao_produto',
        'Frete Grátis': 'frete_gratis',
        'Número FCI': 'numero_fci',
        'Vídeo': 'video',
        'Departamento': 'departamento',
        'Unidade de Medida': 'unidade_medida',
        'Preço de Compra': 'preco_compra',
        'Valor base ICMS ST para retenção': 'valor_base_icms_st',
        'Valor ICMS ST para retenção': 'valor_icms_st',
        'Valor ICMS próprio do substituto': 'valor_icms_proprio_substituto',
        'Categoria do produto': 'categoria',
        'Informações Adicionais': 'informacoes_adicionais',
        'tipoDeSofa': 'tipo_sofa',
        'Grupo Tag': 'grupo_tag',
        'Origem Tag': 'origem_tag'
    }
    df_final = df_final.rename(columns=novos_nomes)

    df_final.to_parquet(CAMINHO_CSVS.parent.parent / 'cache' / 'produtos.parquet', index=False)

    return df_final


if __name__ == '__main__':
    juntando_csvs()