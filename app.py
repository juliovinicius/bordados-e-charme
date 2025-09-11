import base64
from carregadores.google_cloud_storage import salvar_bling_token
from flask import Flask, request
from pathlib import Path
from datetime import datetime, timedelta
import os
import requests
import secrets
import pickle
import json


client_id = '6a98683078ddd386e7702e995261f604ddca8a72'
client_secret = '64e8d1ad698d75e3e1f40e6d94773b11417b4580d961bbdb292dcd5c3b3a'
redirect_uri = 'http://127.0.0.1:8080/redirect'
token_url = 'https://www.bling.com.br/Api/v3/oauth/token'
CAMINHO_PARA_ACCESS_TOKEN = Path(__file__).parent / "cache" / "bling_v3_access_token.b"

app = Flask(__name__)


@app.route('/')
def inicio():
    authorization_code = request.args.get('code')
    return f'Inicio. {authorization_code}'


@app.route('/redirect')
def redirect():
    authorization_code = request.args.get('code')
    state_received = request.args.get('state')

    #return f'Resultado: \nCódigo:{authorization_code}\nState:{state_received}'

    '''# Ensure the received state matches the sent state
    with open('app/state.txt', 'r') as f:
        state = f.read().strip()
        print(state)

    if state_received != state:
        return "State mismatch. Potential CSRF attack.", 400'''

    '''# Step 4: Exchange the authorization code for an access token
    response = requests.post(token_url, data={
        'grant_type': 'authorization_code',
        'code': authorization_code,
        'redirect_uri': redirect_uri,
        'client_id': client_id,
        'client_secret': client_secret
    })

    return response.json()'''

    # Encode client_id and client_secret for Basic Authentication
    client_credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(client_credentials.encode()).decode()
    print(f'1º {client_credentials}\n\n2º {encoded_credentials}')

    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    # Step 4: Exchange the authorization code for an access token
    response = requests.post(token_url, data={
        'grant_type': 'authorization_code',
        'code': authorization_code,
        'redirect_uri': redirect_uri
    }, headers=headers)

    tokens = response.json()
    created_at = datetime.now()
    expires_at = created_at + timedelta(seconds=21600)
    tokens["created_at"] = created_at.isoformat()
    tokens["expires_at"] = expires_at.isoformat()
    '''with open(CAMINHO_PARA_ACCESS_TOKEN, 'wb') as token_file:
        pickle.dump(tokens, token_file)'''
    tokens_json = json.dumps(tokens, ensure_ascii=False, indent=4)
    salvar_bling_token(tokens_json)

    print(f'Resposta do token: {tokens}')
    access_token = tokens.get('access_token')
    token_type = tokens.get('token_type')
    scope_token = tokens.get('scope')
    refresh_token = tokens.get('refresh_token')
    tokens_time = tokens.get('created_at')
    expires_at = tokens.get('expires_at')

    return f'''
        <html>
        <body>
            <p>Access token: {access_token}</p>
            <p>Token type: {token_type}</p>
            <p>Scope: {scope_token}</p>
            <p>Refresh token: {refresh_token}</p>
            <p>Att time: {tokens_time}</p>
            <p>Expires at: {expires_at}</p>
        </body>
        </html>
        ''', 200


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))