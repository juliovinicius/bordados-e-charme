import extratores
import pandas as pd


def aa():
    pedidos = extratores.blingv3.pedidos_gerais()
    servicos_logistica = extratores.blingv3.logistica_servicos()
    sinonimos_servico_logistica = []
    for servico in servicos_logistica['data']:
        if servico['id'] == 10067823520:
            sinonimos_servico_logistica = servico.get('aliases', [])
    print(f'sinonimos: {sinonimos_servico_logistica}')
    pedidos_detalhados = []
    for i, pedido in enumerate(pedidos, start=1):
        dados_do_pedido = extratores.blingv3.obter_pedido(pedido['id'])
        volumes = dados_do_pedido['data']['transporte'].get('volumes', [])
        if volumes and volumes[0]['servico'] in sinonimos_servico_logistica:
            dados_da_entrega = extratores.blingv3.logistica_objeto(volumes[0]['id'])
            dados = {
                'pedido': dados_do_pedido['data'],
                'entrega': dados_da_entrega['data']
            }
            pedidos_detalhados.append(dados)
        if i >= 50:
            break

    return pedidos_detalhados


if __name__ == '__main__':
    aa()
