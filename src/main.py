import logging
import os
import sys

from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication

from config import (
    API_LOG_FILE,
    API_RATE_LIMIT,
    API_RETRY_COUNT,
    API_RETRY_DELAY,
    CACHE_DIR,
    CSV_DIR,
    LOG_DIR,
    LOG_FILE,
    LOG_LEVEL,
    MAP_DOWNLOADS_LOG_FILE,
    MAPS_DIR,
    OSU_API_LOG_LEVEL,
    REPLAY_ISSUES_LOG_FILE,
    RESULTS_DIR,
)
from database import db_close, db_init
from gui import create_gui, show_api_limit_warning
from osu_api import OsuApiClient
from utils import (
    ensure_app_dirs_exist,
    get_env_path,
    get_resource_path,
    mask_path_for_log,
)

env_path = get_env_path()
os.environ["DOTENV_PATH"] = env_path

log_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log_level_map = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}
numeric_level = log_level_map.get(LOG_LEVEL.upper(), logging.INFO)

os.makedirs(LOG_DIR, exist_ok=True)

root_logger = logging.getLogger()
root_logger.setLevel(numeric_level)

for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

try:
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8", mode="w")
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
except Exception as e:
    print(f"Failed to configure logging to file {LOG_FILE}: {e}")

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

api_logger = logging.getLogger("osu_api")
api_log_level = log_level_map.get(OSU_API_LOG_LEVEL.upper(), logging.INFO)
api_logger.setLevel(api_log_level)
api_logger.propagate = False

try:
    api_file_handler = logging.FileHandler(API_LOG_FILE, encoding="utf-8", mode="w")
    api_file_handler.setFormatter(log_formatter)
    api_logger.addHandler(api_file_handler)
except Exception as e:
    print(f"Failed to configure API logging to file {API_LOG_FILE}: {e}")

    logging.error(
        f"API log handler setup failed, API logs will go to console/main log: {e}"
    )
    api_logger.propagate = True

                                          
replay_issues_logger = logging.getLogger("replay_issues")
replay_issues_logger.setLevel(logging.DEBUG)
replay_issues_logger.propagate = False

try:
    issues_file_handler = logging.FileHandler(
        REPLAY_ISSUES_LOG_FILE, encoding="utf-8", mode="w"
    )
    issues_file_handler.setFormatter(log_formatter)
    replay_issues_logger.addHandler(issues_file_handler)
except Exception as e:
    print(
        f"Failed to configure replay issues logging to file {REPLAY_ISSUES_LOG_FILE}: {e}"
    )
    logging.error(
        f"Replay issues log handler setup failed, logs will go to main log: {e}"
    )
    replay_issues_logger.propagate = True

                                     
map_downloads_logger = logging.getLogger("map_downloads")
map_downloads_logger.setLevel(logging.DEBUG)
map_downloads_logger.propagate = False

try:
    downloads_file_handler = logging.FileHandler(
        MAP_DOWNLOADS_LOG_FILE, encoding="utf-8", mode="w"
    )
    downloads_file_handler.setFormatter(log_formatter)
    map_downloads_logger.addHandler(downloads_file_handler)
except Exception as e:
    print(
        f"Failed to configure map downloads logging to file {MAP_DOWNLOADS_LOG_FILE}: {e}"
    )
    logging.error(
        f"Map downloads log handler setup failed, logs will go to main log: {e}"
    )
    map_downloads_logger.propagate = True

logging.info(
    "Logging configured. Main log: %s, API log: %s",
    mask_path_for_log(os.path.normpath(LOG_FILE)),
    mask_path_for_log(os.path.normpath(API_LOG_FILE)),
)
logging.info("Path to .env file: %s", mask_path_for_log(os.path.normpath(env_path)))

osu_api_client = None


def setup_api():
    try:
        token_cache_path = os.path.join(CACHE_DIR, "token_cache.json")
        md5_cache_path = os.path.join(CACHE_DIR, "md5_cache.json")

        api_client = OsuApiClient.get_instance(
            token_cache_path=token_cache_path,
            md5_cache_path=md5_cache_path,
            api_rate_limit=API_RATE_LIMIT,
            api_retry_count=API_RETRY_COUNT,
            api_retry_delay=API_RETRY_DELAY,
        )

        if api_client:
            logging.info("OsuApiClient instance created successfully in setup_api")
        else:
            logging.warning(
                "Failed to create OsuApiClient instance - no API keys found"
            )

        return api_client
    except Exception as e:
        logging.exception(f"Error setting up API client in setup_api: {e}")
        return None


def ensure_directories_exist():
    ensure_app_dirs_exist()

    dirs = [CACHE_DIR, RESULTS_DIR, MAPS_DIR, CSV_DIR]
    for dir_path in dirs:
        try:
            os.makedirs(dir_path, exist_ok=True)
            logging.info("Ensured directory exists: %s", mask_path_for_log(dir_path))
        except Exception as e:
            logging.error(
                "Failed to create directory %s: %s", mask_path_for_log(dir_path), e
            )


def main():
    ensure_directories_exist()

    try:
        db_init()
        logging.info("Database connection initialized")
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        return 1

    app = QApplication(sys.argv)

                                 
    app_icon_path = get_resource_path("app_icon/icon.png")
    if os.path.exists(app_icon_path):
        app_icon = QIcon(app_icon_path)
        app.setWindowIcon(app_icon)
        logging.info("Application icon set successfully")
    else:
        logging.warning(
            f"Application icon not found at: {mask_path_for_log(app_icon_path)}"
        )

    osu_api_client = setup_api()

    show_api_limit_warning()

    create_gui(osu_api_client)
    exit_code = app.exec()

    db_close()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
