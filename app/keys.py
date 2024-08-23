import secrets
import webbrowser

# Define your parameters
client_id = '6a98683078ddd386e7702e995261f604ddca8a72'
response_type = 'code'
state = secrets.token_urlsafe(16)  # Generates a secure random string for state
redirect_uri = 'http://127.0.0.1:8080/redirect'

# Construct the authorization URL
auth_url = (
    f'https://www.bling.com.br/Api/v3/oauth/authorize'
    f'?response_type={response_type}'
    f'&client_id={client_id}'
    f'&state={state}'
    f'&redirect_uri={redirect_uri}'
)

# Save the state to a file for later verification
with open('state.txt', 'w') as f:
    f.write(state)

# Open the authorization URL in the browser
webbrowser.open(auth_url)

#https://app-bling-znz5wp6mbq-uc.a.run.app/?code=a162eeac48c4f907fd9203344348c4f53d6d1af1&state=BhDjLEy8BSOcIhNvboCs2w