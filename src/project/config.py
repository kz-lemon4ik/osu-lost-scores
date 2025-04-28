import os
import logging
from dotenv import load_dotenv
from utils import get_resource_path, mask_path_for_log

logger = logging.getLogger(__name__)

dotenv_path = os.environ.get("DOTENV_PATH")

if not dotenv_path or not os.path.exists(dotenv_path):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dotenv_path = get_resource_path(os.path.join("..", ".env"))
    dotenv_path = os.path.abspath(dotenv_path)
    logger.info(
        "DOTENV_PATH not set or file doesn't exist, using: %s",
        mask_path_for_log(dotenv_path),
    )

if os.path.exists(dotenv_path):
    logger.info("Loading .env from: %s", mask_path_for_log(dotenv_path))
    load_dotenv(dotenv_path=dotenv_path)
else:
    logger.error("Could not find .env file: %s", mask_path_for_log(dotenv_path))

DB_FILE = os.environ.get("DB_FILE", "cache/beatmap_info.db")

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
