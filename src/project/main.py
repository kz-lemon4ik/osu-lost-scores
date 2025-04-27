import logging
import os
import sys

from database import db_init, db_close
from utils import get_resource_path, mask_path_for_log

from PySide6.QtWidgets import QApplication
from gui import create_gui

env_path = get_resource_path(os.path.join("..", ".env"))
os.environ["DOTENV_PATH"] = env_path

LOG_FILENAME = get_resource_path(os.path.join("..", "log.txt"))

log_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)-5.5s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

try:
    file_handler = logging.FileHandler(LOG_FILENAME, encoding="utf-8", mode="w")
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
except Exception as e:
    print(f"Failed to configure logging to file {LOG_FILENAME}: {e}")

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

logging.info(
    "Logging configured. Output to console and file %s",
    mask_path_for_log(os.path.normpath(LOG_FILENAME)),
)
logging.info("Path to .env file: %s", mask_path_for_log(os.path.normpath(env_path)))


def setup_api():
    try:
        from osu_api import get_keys_from_keyring

        client_id, client_secret = get_keys_from_keyring()
        if not client_id or not client_secret:
            logging.warning(
                "API keys not configured. Will prompt to enter them through the interface."
            )
            return True

        return True
    except Exception as e:
        logging.exception(f"Error setting up API: {e}")
        return True


def main():
    try:
        db_init()
        logging.info("Database connection initialized")
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        return 1

    app = QApplication(sys.argv)

    if not setup_api():
        db_close()
        return 1

    create_gui()

    exit_code = app.exec()

    db_close()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
