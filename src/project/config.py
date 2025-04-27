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

cutoff_env = os.environ.get("CUTOFF_DATE", "1729728000")

try:
    CUTOFF_DATE = int(cutoff_env)
except ValueError:
    logger.warning(
        "Could not convert CUTOFF_DATE '%s' to number, using default value", cutoff_env
    )
    CUTOFF_DATE = 1729728000
