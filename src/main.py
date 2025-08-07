import datetime
import logging
import os
import shutil
import sys
from pathlib import Path

from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication

from app_config import (
    API_RATE_LIMIT,
    API_RETRY_COUNT,
    API_RETRY_DELAY,
    LOG_DIR,
    LOG_LEVEL,
    OSU_API_LOG_LEVEL,
)
from database import db_close, db_init
from gui import create_gui, show_api_limit_warning
from osu_api import OsuApiClient
from path_utils import get_env_path, get_standard_dir, mask_path_for_log


def cleanup_old_app_logs(base_log_directory_str: str, days_to_keep: int = 7):
    logger = logging.getLogger("root")
    logger.info(
        f"Cleaning up app log subdirectories older than {days_to_keep} days in {mask_path_for_log(base_log_directory_str)}..."
    )
    base_log_directory = Path(base_log_directory_str)
    cutoff_time = datetime.datetime.now() - datetime.timedelta(days=days_to_keep)
    cleaned_count = 0
    if not base_log_directory.exists():
        logger.info(
            f"Base log directory {mask_path_for_log(str(base_log_directory))} does not exist. Nothing to clean"
        )
        return
    try:
        for item in base_log_directory.iterdir():
            if item.is_dir():
                try:
                    dir_time = datetime.datetime.strptime(
                        item.name, "%Y-%m-%d_%H-%M-%S"
                    )
                    if dir_time < cutoff_time:
                        shutil.rmtree(item)
                        logger.info(f"Deleted old app log directory: {item.name}")
                        cleaned_count += 1
                except ValueError:
                    logger.debug(
                        f"Skipping directory (name not a parsable timestamp): {item.name}"
                    )
                except Exception as del_exc:
                    logger.error(
                        f"Error deleting old app log directory {item.name}: {del_exc}"
                    )
    except Exception as cleanup_err:
        logger.error(
            f"Error iterating through log directory {mask_path_for_log(str(base_log_directory))}: {cleanup_err}"
        )
    logger.info(
        f"App log cleanup finished. Deleted {cleaned_count} old log directories"
    )


def setup_file_logger(logger_name, file_path, level=logging.DEBUG, formatter=None):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = False

    for existing_handler in logger.handlers[:]:
        logger.removeHandler(existing_handler)
        existing_handler.close()

    try:
        new_file_handler = logging.FileHandler(file_path, encoding="utf-8", mode="w")
        if formatter:
            new_file_handler.setFormatter(formatter)
        logger.addHandler(new_file_handler)
    except Exception as log_setup_err:
        logging.error(
            f"Failed to configure logger '{logger_name}' for file {file_path}: {log_setup_err}"
        )

    return logger


env_path = get_env_path()
os.environ["DOTENV_PATH"] = env_path
if not os.path.exists(LOG_DIR):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        print(
            f"INITIAL_SETUP: Base log directory created at {mask_path_for_log(LOG_DIR)}"
        )
    except Exception as e:
        print(
            f"INITIAL_SETUP_ERROR: Could not create base log directory {mask_path_for_log(LOG_DIR)}: {e}"
        )
current_run_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
RUN_SPECIFIC_LOG_DIR = os.path.join(LOG_DIR, current_run_timestamp)
try:
    os.makedirs(RUN_SPECIFIC_LOG_DIR, exist_ok=True)
except Exception as e:
    print(
        f"CRITICAL_LOG_SETUP_ERROR: Could not create run-specific log directory {mask_path_for_log(RUN_SPECIFIC_LOG_DIR)}: {e}. Logging to base log directory"
    )
    RUN_SPECIFIC_LOG_DIR = LOG_DIR
LOG_FILE_path = os.path.join(RUN_SPECIFIC_LOG_DIR, "log.txt")
API_LOG_FILE_path = os.path.join(RUN_SPECIFIC_LOG_DIR, "api_log.txt")
REPLAY_PROCESSING_DETAILS_LOG_FILE_path = os.path.join(
    RUN_SPECIFIC_LOG_DIR, "replay_processing_details.txt"
)
ASSET_DOWNLOADS_LOG_FILE_path = os.path.join(
    RUN_SPECIFIC_LOG_DIR, "asset_downloads.txt"
)
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
api_log_level = log_level_map.get(OSU_API_LOG_LEVEL.upper(), logging.INFO)
root_logger = logging.getLogger()
root_logger.setLevel(numeric_level)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
    handler.close()
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)
cleanup_old_app_logs(LOG_DIR)
try:
    file_handler = logging.FileHandler(LOG_FILE_path, encoding="utf-8", mode="w")
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
except Exception as e:
    logging.error(f"Failed to configure main file logging to {LOG_FILE_path}: {e}")

setup_file_logger("api_logger", API_LOG_FILE_path, api_log_level, log_formatter)

setup_file_logger(
    "replay_processing_details",
    REPLAY_PROCESSING_DETAILS_LOG_FILE_path,
    logging.DEBUG,
    log_formatter,
)
setup_file_logger(
    "asset_downloads",
    ASSET_DOWNLOADS_LOG_FILE_path,
    logging.DEBUG,
    log_formatter,
)

logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("PIL").setLevel(logging.INFO)

logging.info(
    "Logging configured. Session logs in: %s",
    mask_path_for_log(os.path.normpath(RUN_SPECIFIC_LOG_DIR)),
)


def setup_api():
    try:
        token_cache_path = get_standard_dir("cache/token_cache.json")
        api_client = OsuApiClient.get_instance(
            token_cache_path=token_cache_path,
            api_rate_limit=API_RATE_LIMIT,
            api_retry_count=API_RETRY_COUNT,
            api_retry_delay=API_RETRY_DELAY,
        )
        if api_client:
            logging.info("OsuApiClient instance created successfully in setup_api")
        else:
            logging.warning("Failed to create OsuApiClient instance")
        return api_client
    except Exception as api_setup_err:
        logging.exception(f"Error setting up API client in setup_api: {api_setup_err}")
        return None


def main():
    try:
        db_init()
        logging.info("Database connection initialized")
    except Exception as db_init_err:
        logging.error(f"Failed to initialize database: {db_init_err}")
        sys.exit(1)

    app = QApplication.instance() or QApplication(sys.argv)

    # Configure tooltip delay to 1 second (1000ms)
    # Note: PySide6 doesn't directly support tooltip delay configuration like this
    # The delay is controlled by the OS, but we can work around it in the GUI

    app_icon_path = get_standard_dir("assets/images/app_icon/icon.ico")
    if os.path.exists(app_icon_path) and isinstance(app, QApplication):
        app_icon = QIcon(app_icon_path)
        app.setWindowIcon(app_icon)
        logging.info("Application icon set successfully")
    else:
        logging.warning(
            f"Application icon not found at: {mask_path_for_log(app_icon_path)}"
        )

    current_api_client = setup_api()

    main_window, _ = create_gui(current_api_client)

    main_window.show()

    show_api_limit_warning()

    exit_code = app.exec()
    db_close()
    logging.info("Application shutting down. Exit code: %s", exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
