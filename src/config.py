import os
import logging
from dotenv import load_dotenv
from utils import get_env_path, get_standard_dir, mask_path_for_log

logger = logging.getLogger(__name__)

dotenv_path = os.environ.get("DOTENV_PATH")

if not dotenv_path or not os.path.exists(dotenv_path):
    dotenv_path = get_env_path()
    logger.info(
        "DOTENV_PATH not set or file doesn't exist, using: %s",
        mask_path_for_log(dotenv_path),
    )

if os.path.exists(dotenv_path):
    logger.info("Loading .env from: %s", mask_path_for_log(dotenv_path))
    load_dotenv(dotenv_path=dotenv_path)
else:
    logger.error("Could not find .env file: %s", mask_path_for_log(dotenv_path))

                                                                              
CACHE_DIR = os.environ.get("CACHE_DIR", get_standard_dir("cache"))
RESULTS_DIR = os.environ.get("RESULTS_DIR", get_standard_dir("results"))
MAPS_DIR = os.environ.get("MAPS_DIR", get_standard_dir("maps"))
CSV_DIR = os.environ.get("CSV_DIR", get_standard_dir("csv"))
LOG_DIR = get_standard_dir("log")

                                
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(MAPS_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

                    
DB_FILE = os.environ.get("DB_FILE", os.path.join(CACHE_DIR, "beatmap_info.db"))

cutoff_env = os.environ.get("CUTOFF_DATE", "1730114220")

try:
    CUTOFF_DATE = int(cutoff_env)
except ValueError:
    logger.warning(
        "Could not convert CUTOFF_DATE '%s' to number, using default value", cutoff_env
    )
    CUTOFF_DATE = 1730114220

thread_pool_env = os.environ.get("THREAD_POOL_SIZE", "16")
try:
    THREAD_POOL_SIZE = int(thread_pool_env)
except ValueError:
    logger.warning(
        "Could not convert THREAD_POOL_SIZE '%s' to number, using default value",
        thread_pool_env,
    )
    THREAD_POOL_SIZE = 16

                                                   
io_thread_pool_env = os.environ.get(
    "IO_THREAD_POOL_SIZE", str(os.cpu_count() * 2 or 32)
)
try:
    IO_THREAD_POOL_SIZE = int(io_thread_pool_env)
except ValueError:
    logger.warning(
        "Could not convert IO_THREAD_POOL_SIZE '%s' to number, using default value",
        io_thread_pool_env,
    )
                                                                         
    IO_THREAD_POOL_SIZE = min(32, os.cpu_count() * 2 or 16)

gui_thread_pool_env = os.environ.get("GUI_THREAD_POOL_SIZE", "24")
try:
    GUI_THREAD_POOL_SIZE = int(gui_thread_pool_env)
except ValueError:
    logger.warning(
        "Could not convert GUI_THREAD_POOL_SIZE '%s' to number, using default value",
        gui_thread_pool_env,
    )
    GUI_THREAD_POOL_SIZE = 24

map_download_timeout_env = os.environ.get("MAP_DOWNLOAD_TIMEOUT", "30")
try:
    MAP_DOWNLOAD_TIMEOUT = int(map_download_timeout_env)
except ValueError:
    logger.warning(
        "Could not convert MAP_DOWNLOAD_TIMEOUT '%s' to number, using default value",
        map_download_timeout_env,
    )
    MAP_DOWNLOAD_TIMEOUT = 30

download_retry_count_env = os.environ.get("DOWNLOAD_RETRY_COUNT", "3")
try:
    DOWNLOAD_RETRY_COUNT = int(download_retry_count_env)
except ValueError:
    logger.warning(
        "Could not convert DOWNLOAD_RETRY_COUNT '%s' to number, using default value",
        download_retry_count_env,
    )
    DOWNLOAD_RETRY_COUNT = 3

                                                                        
CACHE_DIR = os.path.normpath(CACHE_DIR)
RESULTS_DIR = os.path.normpath(RESULTS_DIR)
MAPS_DIR = os.path.normpath(MAPS_DIR)
CSV_DIR = os.path.normpath(CSV_DIR)

logger.info(
    "Configured paths: CACHE_DIR=%s, RESULTS_DIR=%s, MAPS_DIR=%s, CSV_DIR=%s",
    mask_path_for_log(CACHE_DIR),
    mask_path_for_log(RESULTS_DIR),
    mask_path_for_log(MAPS_DIR),
    mask_path_for_log(CSV_DIR),
)

                       
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_DIR = os.environ.get("LOG_DIR", get_standard_dir("log"))
LOG_FILE = os.path.join(LOG_DIR, "log.txt")
API_LOG_FILE = os.path.join(LOG_DIR, "api_log.txt")

                             
os.makedirs(LOG_DIR, exist_ok=True)

logger.info(
    "Configured logging: LOG_LEVEL=%s, LOG_DIR=%s, LOG_FILE=%s, API_LOG_FILE=%s",
    LOG_LEVEL,
    mask_path_for_log(LOG_DIR),
    mask_path_for_log(LOG_FILE),
    mask_path_for_log(API_LOG_FILE),
)

                   
api_requests_per_minute_env = os.environ.get("API_REQUESTS_PER_MINUTE", "60")
api_retry_count_env = os.environ.get("API_RETRY_COUNT", "3")
api_retry_delay_env = os.environ.get("API_RETRY_DELAY", "0.5")

try:
    API_REQUESTS_PER_MINUTE = int(api_requests_per_minute_env)
                          
    if API_REQUESTS_PER_MINUTE <= 0:
        logger.warning(
            "API_REQUESTS_PER_MINUTE set to %d, treating as unlimited. This is dangerous!",
            API_REQUESTS_PER_MINUTE,
        )
        API_RATE_LIMIT = 0.0                                          
    else:
                                                            
        API_RATE_LIMIT = 60.0 / API_REQUESTS_PER_MINUTE
except ValueError:
    logger.warning(
        "Could not convert API_REQUESTS_PER_MINUTE '%s' to number, using default value",
        api_requests_per_minute_env,
    )
    API_REQUESTS_PER_MINUTE = 60
    API_RATE_LIMIT = 1.0

try:
    API_RETRY_COUNT = int(api_retry_count_env)
except ValueError:
    logger.warning(
        "Could not convert API_RETRY_COUNT '%s' to number, using default value",
        api_retry_count_env,
    )
    API_RETRY_COUNT = 3

try:
    API_RETRY_DELAY = float(api_retry_delay_env)
except ValueError:
    logger.warning(
        "Could not convert API_RETRY_DELAY '%s' to number, using default value",
        api_retry_delay_env,
    )
    API_RETRY_DELAY = 0.5

logger.info(
    "Configured API settings: API_REQUESTS_PER_MINUTE=%d, API_RETRY_COUNT=%s, API_RETRY_DELAY=%s",
    API_REQUESTS_PER_MINUTE,
    API_RETRY_COUNT,
    API_RETRY_DELAY,
)
