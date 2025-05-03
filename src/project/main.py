import logging
import os
import sys

from database import db_init, db_close
from utils import get_resource_path, mask_path_for_log
from config import LOG_LEVEL, LOG_FILE

from PySide6.QtWidgets import QApplication
from gui import create_gui

env_path = get_resource_path(os.path.join("..", ".env"))
os.environ["DOTENV_PATH"] = env_path

LOG_FILENAME = LOG_FILE

log_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log_level_map = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}
numeric_level = log_level_map.get(LOG_LEVEL.upper(), logging.INFO)

                                  
logs_dir = os.path.dirname(LOG_FILENAME)
os.makedirs(logs_dir, exist_ok=True)

                   
API_LOG_FILENAME = os.path.join(logs_dir, "api_log.txt")

                             
root_logger = logging.getLogger()
root_logger.setLevel(numeric_level)

                                   
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

                                                                
class ExcludeAPILogFilter(logging.Filter):
    def filter(self, record):
        return not record.name.startswith('osu_api_calls')

                                       
try:
    file_handler = logging.FileHandler(LOG_FILENAME, encoding="utf-8", mode="w")
    file_handler.setFormatter(log_formatter)
    file_handler.addFilter(ExcludeAPILogFilter())                    
    root_logger.addHandler(file_handler)
except Exception as e:
    print(f"Failed to configure logging to file {LOG_FILENAME}: {e}")

                               
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.addFilter(ExcludeAPILogFilter())                                         
root_logger.addHandler(console_handler)

                       
api_logger = logging.getLogger('osu_api_calls')
api_logger.setLevel(logging.DEBUG)                                               
api_logger.propagate = False                                                    

try:
    api_file_handler = logging.FileHandler(API_LOG_FILENAME, encoding="utf-8", mode="w")
    api_file_handler.setFormatter(log_formatter)
    api_logger.addHandler(api_file_handler)
except Exception as e:
    print(f"Failed to configure API logging to file {API_LOG_FILENAME}: {e}")
                                                                                     
    api_logger.propagate = True

logging.info(
    "Logging configured. Main log: %s, API log: %s",
    mask_path_for_log(os.path.normpath(LOG_FILENAME)),
    mask_path_for_log(os.path.normpath(API_LOG_FILENAME))
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


def ensure_directories_exist():
    from config import CACHE_DIR, RESULTS_DIR, MAPS_DIR, CSV_DIR

    dirs = [CACHE_DIR, RESULTS_DIR, MAPS_DIR, CSV_DIR]
    for dir_path in dirs:
        try:
            os.makedirs(dir_path, exist_ok=True)
            logging.info("Ensured directory exists: %s", mask_path_for_log(dir_path))
        except Exception as e:
            logging.error("Failed to create directory %s: %s", mask_path_for_log(dir_path), e)


def main():
    ensure_directories_exist()

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

                                                                            
    from gui import show_api_limit_warning
    show_api_limit_warning()

    window = create_gui()
    exit_code = app.exec()

    db_close()

    return exit_code

if __name__ == "__main__":
    sys.exit(main())
