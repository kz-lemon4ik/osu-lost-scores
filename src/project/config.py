import os
from dotenv import load_dotenv

                                        
load_dotenv()

CLIENT_ID = os.environ.get("CLIENT_ID", "default_client_id")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "default_client_secret")
PERFORMANCE_CALCULATOR_PATH = os.environ.get(
    "PERFORMANCE_CALCULATOR_PATH",
    "../../osu-tools/PerformanceCalculator/bin/Release/net8.0/PerformanceCalculator.exe"
)
DB_FILE = os.environ.get("DB_FILE", "../cache/beatmap_info.db")
CUTOFF_DATE = os.environ.get("CUTOFF_DATE", "28 Oct 2024")
