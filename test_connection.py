import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

try:
    conn = psycopg2.connect(
        dbname=os.getenv("SPOTIFY_DB"),
        user=os.getenv("SPOTIFY_DB_USER"),
        password=os.getenv("SPOTIFY_DB_PASS"),
        host="127.0.0.1",
        port=os.getenv("DB_PORT")
    )
    print("Connected successfully to spotify_db as spotify_user!")
    conn.close()
except Exception as e:
    print("Connection failed:", e)
