#!/usr/bin/env python3
"""
TikTok OAuth Helper
--------------------
Opens the browser, you log in with @syncinUS, copy the redirect URL,
and the script saves the access token to .env automatically.

Usage:
  python3 auth_tiktok.py
"""

import base64
import hashlib
import os
import secrets
import urllib.parse
import webbrowser
from pathlib import Path

import requests
from dotenv import load_dotenv, set_key

load_dotenv()

CLIENT_KEY    = os.environ.get("TIKTOK_CLIENT_KEY", "")
CLIENT_SECRET = os.environ.get("TIKTOK_CLIENT_SECRET", "")
REDIRECT_URI  = "https://example.com/callback"
SCOPES        = "user.info.basic,video.publish,video.upload"
ENV_FILE      = Path(__file__).parent / ".env"


def generate_pkce() -> tuple[str, str]:
    code_verifier  = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    digest         = hashlib.sha256(code_verifier.encode()).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return code_verifier, code_challenge


def get_access_token(auth_code: str, code_verifier: str) -> dict:
    url  = "https://open.tiktokapis.com/v2/oauth/token/"
    data = {
        "client_key":    CLIENT_KEY,
        "client_secret": CLIENT_SECRET,
        "code":          auth_code,
        "grant_type":    "authorization_code",
        "redirect_uri":  REDIRECT_URI,
        "code_verifier": code_verifier,
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()


def main():
    if not CLIENT_KEY or not CLIENT_SECRET:
        print("ERROR: TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET are missing in .env!")
        return

    code_verifier, code_challenge = generate_pkce()

    params = urllib.parse.urlencode({
        "client_key":            CLIENT_KEY,
        "scope":                 SCOPES,
        "response_type":         "code",
        "redirect_uri":          REDIRECT_URI,
        "code_challenge":        code_challenge,
        "code_challenge_method": "S256",
    })
    auth_url = f"https://www.tiktok.com/v2/auth/authorize/?{params}"

    print("\nTikTok OAuth Flow — @syncinUS")
    print("=" * 50)
    print("STEP 1: Make sure 'https://example.com/callback' is set as Redirect URI")
    print("        in the TikTok Developer Portal under Login Kit → Web.\n")
    print("STEP 2: The browser will open — log in with the @syncinUS account")
    print("        and grant the requested permissions.\n")
    print("STEP 3: You'll be redirected to example.com (the page may show an error")
    print("        — that's expected).\n")
    print("STEP 4: Copy the FULL URL from the address bar and paste it here.\n")
    print("=" * 50)

    input("Press ENTER to open the browser...")
    webbrowser.open(auth_url)

    print("\nAfter authorizing, you'll be redirected to example.com.")
    print("The URL looks something like:")
    print("  https://example.com/callback?code=XXXX&scopes=...&state=...\n")

    redirect_url = input("Paste the full URL here: ").strip()

    parsed     = urllib.parse.urlparse(redirect_url)
    params_dict = urllib.parse.parse_qs(parsed.query)
    auth_code  = params_dict.get("code", [None])[0]

    if not auth_code:
        print("\nERROR: No 'code' found in the URL.")
        print("Make sure you copied the complete URL from the address bar.")
        return

    print(f"\nAuth code found. Fetching access token...")
    token_data = get_access_token(auth_code, code_verifier)

    access_token = token_data.get("access_token")
    open_id      = token_data.get("open_id")

    if not access_token:
        print(f"\nERROR fetching token: {token_data}")
        return

    if not ENV_FILE.exists():
        ENV_FILE.write_text("")
    set_key(str(ENV_FILE), "TIKTOK_ACCESS_TOKEN", access_token)
    if open_id:
        set_key(str(ENV_FILE), "TIKTOK_OPEN_ID", open_id)

    print(f"\nToken saved successfully to .env!")
    print(f"You can now run: python3 main.py")


if __name__ == "__main__":
    main()
