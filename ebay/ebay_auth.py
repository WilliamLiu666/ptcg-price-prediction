import base64
import time
import requests


class EbayAuth:
    def __init__(self, client_id: str, client_secret: str, sandbox: bool = True) -> None:
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.sandbox = sandbox

        self.access_token: str | None = None
        self.expires_at: float = 0

    @property
    def token_url(self) -> str:
        if self.sandbox:
            return "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
        return "https://api.ebay.com/identity/v1/oauth2/token"

    def get_access_token(self, scope: str = "https://api.ebay.com/oauth/api_scope") -> str:
        now = time.time()

        if self.access_token and (now < (self.expires_at - 60)):
            return self.access_token

        raw = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(raw.encode("utf-8")).decode("utf-8")

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {encoded}",
        }

        data = {
            "grant_type": "client_credentials",
            "scope": scope,
        }

        resp = requests.post(self.token_url, headers=headers, data=data, timeout=30)
        resp.raise_for_status()

        token_data = resp.json()
        access_token = token_data.get("access_token")
        if access_token is None:
            raise ValueError("Access token not found in response")
        self.access_token = access_token
        expires_in = token_data.get("expires_in", 7200)
        self.expires_at = now + expires_in

        return self.access_token if self.access_token is not None else ""
    
if __name__ == "__main__":
    CLIENT_ID = ""
    CLIENT_SECRET = ""

    auth = EbayAuth(CLIENT_ID, CLIENT_SECRET, sandbox=False)

    token = auth.get_access_token()

    print("Access Token:")
    print(token)
    print("Expires At:")
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(auth.expires_at)))