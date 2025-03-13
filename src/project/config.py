import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.environ.get("CLIENT_ID", "default_client_id")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "default_client_secret")
DB_FILE = os.environ.get("DB_FILE", "../cache/beatmap_info.db")
CUTOFF_DATE = os.environ.get("CUTOFF_DATE", "28 Oct 2024")
