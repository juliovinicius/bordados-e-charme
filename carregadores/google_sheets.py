from pathlib import Path

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

import transformadores, extratores


CAMINHO_PARA_CREDENCIAIS_DO_SHEETS = Path(__file__).parent.parent / "credenciais" / "google_sheets_credential.json"


def alimentar_planilha(dados: pd.DataFrame):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CAMINHO_PARA_CREDENCIAIS_DO_SHEETS, scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key('19-miGGqp-kjINZeTd0ZClZFfxuCSBbyXgbb9ORW9be4')

    sheet = spreadsheet.get_worksheet_by_id(697583497)

    set_with_dataframe(sheet, dados)

    return 'Dataframe escrito no Google Sheets!'


if __name__ == '__main__':
    alimentar_planilha(transformadores.estrutura_produto.multiplos(
        extratores.bling.todos_os_produtos()
    ))