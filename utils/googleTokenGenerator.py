from pathlib import Path
import os
import json

from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow

# Load .env from the project root
load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
TOKEN_PATH = Path("gmail_token.json")

def main():
    raw = os.getenv("GOOGLE_OAUTH_CLIENT_JSON")
    if not raw:
        raise RuntimeError(
            "GOOGLE_OAUTH_CLIENT_JSON not set. Put your OAuth client JSON into .env:\n"
            "GOOGLE_OAUTH_CLIENT_JSON='{\"installed\":{...}}'"
        )

    try:
        cfg = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"Failed to parse GOOGLE_OAUTH_CLIENT_JSON: {e}")

    client = cfg.get("installed") or cfg.get("web")
    required = {"client_id", "client_secret", "auth_uri", "token_uri"}
    if not client or not required.issubset(client.keys()):
        raise RuntimeError(
            "GOOGLE_OAUTH_CLIENT_JSON missing required fields "
            "(client_id, client_secret, auth_uri, token_uri)"
        )

    flow = InstalledAppFlow.from_client_config(
        {"web": client},
        SCOPES,
        redirect_uri="http://localhost:8080"
    )
    creds = flow.run_local_server(port=8080)
    TOKEN_PATH.write_text(creds.to_json())
    print("Saved token to", TOKEN_PATH)

if __name__ == "__main__":
    main()