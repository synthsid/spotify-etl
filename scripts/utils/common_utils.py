import os
import time 
import json
import base64
import psycopg2 
import requests 
from dotenv import load_dotenv 

# Need to load environment variables 

load_dotenv()

# -------- Spotify API UTILS ---------- 

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

def base64_credentials():
    """Encodes client ID and secret in base64 for Spotify token exchange."""
    creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
    return base64.b64encode(creds.encode()).decode()

def get_token():
    """Fetches Spotify API token using Client Credentials flow."""
    url = 'https://accounts.spotify.com/api/token'
    headers = {
        "Authorization": "Basic " + base64_credentials(),
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def auth_header(token):
    """Returns the bearer auth header for Spotify API."""
    return {"Authorization": f"Bearer {token}"}

def safe_request(url, headers, params=None):
    """
    Spotify API-safe GET request.
    Handles rate limiting via 429 and retries with Retry-After delay.
    """
    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 1))
            print(f"Rate limited. Retrying in {wait} seconds...")
            time.sleep(wait + 1)
        else:
            response.raise_for_status()
            return response.json()

# ========== POSTGRES CONNECTION UTILS ==========

def get_connection():
    """Creates and returns a psycopg2 connection to the Spotify Postgres DB."""
    return psycopg2.connect(
        dbname=os.getenv("SPOTIFY_DB"),
        user=os.getenv("SPOTIFY_DB_USER"),
        password=os.getenv("SPOTIFY_DB_PASS"),
        host=os.getenv("LOCAL_DB_HOST"),
        port=os.getenv("LOCAL_DB_PORT")
    )