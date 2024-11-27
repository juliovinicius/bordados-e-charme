from pathlib import Path

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

import transformadores, extratores


CAMINHO_PARA_CREDENCIAIS_DO_SHEETS = Path(__file__).parent.parent / "credenciais" / "chave-google-sheets.json"


def alimentar_planilha(dados: pd.DataFrame):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CAMINHO_PARA_CREDENCIAIS_DO_SHEETS, scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key('1pLo-EtDcLuMEUEPE272rl9GDC9wLa6BaNUxipRXJdko')

    sheet = spreadsheet.get_worksheet_by_id(1763687526)

    sheet.clear()
    set_with_dataframe(sheet, dados)

    return 'Dataframe escrito no Google Sheets!'


if __name__ == '__main__':
    '''alimentar_planilha(transformadores.estrutura_pedido.agrupando_por_canal(transformadores.estrutura_pedido.multiplos(
        extratores.bling.todos_os_pedidos()
    )))'''
    '''alimentar_planilha(transformadores.estrutura_produto.multiplos(
        extratores.bling.todos_os_produtos()
    ))'''
    apikey1 = 'd2713e9ab254bee05f94f7d72a2cdc23d2df71e0370aefde2fd7b0b8b1c06338'
    apikey2 = 'be5001dee4cad6ed552b50096dd022b132bf209417c5469b8360f395272cd74c'
    apikey3 = '6f646fa987f2b6fa2bb8fd50024eba933c6befc0c0d37b395388b4107f34440b'
    apikey4 = '882086a25329f3c81061baa3159f521df591d629aa4a57651b87f6ab180dd6b4'
    apis = [apikey1, apikey2, apikey3, apikey4]
    alimentar_planilha(transformadores.estrutura_contas_a_receber.multiplas_razoes_sociais(
        apis
    ))
