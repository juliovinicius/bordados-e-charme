import extratores, transformadores, carregadores


def gatilho_de_att_de_pedidos():
    novos_pedidos, todos_os_pedidos = transformadores.estrutura_pedido.multiplos_pedidos(
        extratores.blingv3.pedidos_gerais()
    )
    carregadores.google_cloud_storage.salvando_no_gcs(novos_pedidos, todos_os_pedidos)

    return 'Fluxo completo com sucesso'


if __name__ == '__main__':
    gatilho_de_att_de_pedidos()
