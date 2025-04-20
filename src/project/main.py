import logging
import atexit
import os
import sys
from utils import get_resource_path

env_path = get_resource_path(os.path.join("..", ".env"))
os.environ["DOTENV_PATH"] = env_path

LOG_FILENAME = get_resource_path(os.path.join("..", "log.txt"))

log_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)-5.5s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

try:
    file_handler = logging.FileHandler(LOG_FILENAME, encoding='utf-8', mode='w')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
except Exception as e:
    print(f"Failed to configure logging to file {LOG_FILENAME}: {e}")

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

logging.info("Logging configured. Output to console and file %s", LOG_FILENAME)
logging.info(f"Path to .env file: {env_path}")

from PySide6.QtWidgets import QApplication
from gui import create_gui


def get_resource_path(relative_path):
                                            
    if hasattr(sys, '_MEIPASS'):
                               
        return os.path.join(sys._MEIPASS, relative_path)
    else:
                                  
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', relative_path)

def setup_api():
    try:
        from osu_api import setup_api_keys, restore_env_defaults

        if not setup_api_keys():
            logging.warning("API keys not configured. Will prompt to enter them through the interface.")
            return True

        return True
    except Exception as e:
        logging.exception(f"Error setting up API: {e}")
        return True


def main():
    app = QApplication(sys.argv)

    if not setup_api():
        return 1

    window = create_gui()

    return app.exec()


if __name__ == "__main__":
    sys.exit(main())
