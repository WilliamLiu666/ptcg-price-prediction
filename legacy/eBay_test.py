import base64
import requests


CLIENT_ID = ""
CLIENT_SECRET = ""

url = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"

raw = f"{CLIENT_ID}:{CLIENT_SECRET}"
encoded = base64.b64encode(raw.encode()).decode()

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": f"Basic {encoded}",
}

data = {
    "grant_type": "client_credentials",
    "scope": "https://api.ebay.com/oauth/api_scope",
}

resp = requests.post(url, headers=headers, data=data, timeout=30)
print(resp.status_code)
print(resp.text)