import configparser
import logging
import os

from path_utils import get_settings_path, get_standard_dir, mask_path_for_log

logger = logging.getLogger(__name__)

SETTINGS_PATH = get_settings_path()

DEFAULT_SETTINGS: dict[str, dict[str, str]] = {
    "analysis": {"cutoff_date": "1730114220"},
    "logging": {"level": "DEBUG", "osu_api_level": "INFO"},
    "database": {"file": "beatmap_info.db"},
    "paths": {
        "cache_dir": "cache",
        "maps_dir": "cache/maps",
        "log_dir": "cache/logs",
        "results_dir": "results",
    },
    "performance": {
        "gui_thread_pool_size": "24",
        "thread_pool_size": "16",
        "io_thread_pool_size": "32",
    },
    "download": {
        "map_download_timeout": "30",
        "download_retry_count": "3",
        "check_missing_beatmap_ids": "false",
    },
    "api": {
        "requests_per_minute": "60",
        "retry_count": "3",
        "retry_delay": "0.5",
    },
    "oauth": {
        "backend_base_url": "https://api.lemon4ik.kz",
        "frontend_base_url": "https://lost.lemon4ik.kz",
    },
    "gui": {
        "osu_path": "",
        "username": "",
        "scores_count": "",
        "include_unranked": "false",
        "check_missing_ids": "false",
        "show_lost": "false",
        "avatar_path": "",
    },
}


def _new_parser() -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    parser.optionxform = str
    return parser


def _load_settings() -> configparser.ConfigParser:
    parser = _new_parser()
    parser.read_dict(DEFAULT_SETTINGS)

    if os.path.exists(SETTINGS_PATH):
        parser.read(SETTINGS_PATH, encoding="utf-8")
    else:
        logger.warning(
            "settings.ini not found at %s, falling back to built-in defaults",
            mask_path_for_log(SETTINGS_PATH),
        )
    return parser


SETTINGS = _load_settings()

PUBLIC_REQUESTS_PER_MINUTE = 1200

cache_dir_name = SETTINGS.get("paths", "cache_dir", fallback="cache")
maps_dir_name = SETTINGS.get(
    "paths", "maps_dir", fallback=os.path.join(cache_dir_name, "maps")
)
log_dir_name = SETTINGS.get(
    "paths", "log_dir", fallback=os.path.join(cache_dir_name, "logs")
)
results_dir_name = SETTINGS.get("paths", "results_dir", fallback="results")

CACHE_DIR = get_standard_dir(cache_dir_name)
MAPS_DIR = get_standard_dir(maps_dir_name)
LOG_DIR = get_standard_dir(log_dir_name)
RESULTS_DIR = get_standard_dir(results_dir_name)

AVATAR_DIR = os.path.join(CACHE_DIR, "avatars")
COVER_DIR = os.path.join(CACHE_DIR, "covers")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(MAPS_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(AVATAR_DIR, exist_ok=True)
os.makedirs(COVER_DIR, exist_ok=True)


def _get_int(section: str, option: str, fallback: int) -> int:
    try:
        return SETTINGS.getint(section, option, fallback=fallback)
    except ValueError:
        logger.warning(
            "Invalid integer for %s.%s, falling back to %s",
            section,
            option,
            fallback,
        )
        return fallback


def _get_float(section: str, option: str, fallback: float) -> float:
    try:
        return SETTINGS.getfloat(section, option, fallback=fallback)
    except ValueError:
        logger.warning(
            "Invalid float for %s.%s, falling back to %s",
            section,
            option,
            fallback,
        )
        return fallback


def _get_bool(section: str, option: str, fallback: bool) -> bool:
    try:
        return SETTINGS.getboolean(section, option, fallback=fallback)
    except ValueError:
        logger.warning(
            "Invalid boolean for %s.%s, falling back to %s",
            section,
            option,
            fallback,
        )
        return fallback


_db_filename = SETTINGS.get("database", "file", fallback="beatmap_info.db")
DB_FILE = (
    _db_filename
    if os.path.isabs(_db_filename)
    else os.path.join(CACHE_DIR, _db_filename)
)

CUTOFF_DATE = _get_int("analysis", "cutoff_date", 1730114220)
THREAD_POOL_SIZE = _get_int("performance", "thread_pool_size", 16)
IO_THREAD_POOL_SIZE = _get_int(
    "performance", "io_thread_pool_size", (os.cpu_count() or 8) * 2
)
IO_THREAD_POOL_SIZE = min(32, IO_THREAD_POOL_SIZE)
GUI_THREAD_POOL_SIZE = _get_int("performance", "gui_thread_pool_size", 24)
MAP_DOWNLOAD_TIMEOUT = _get_int("download", "map_download_timeout", 30)
DOWNLOAD_RETRY_COUNT = _get_int("download", "download_retry_count", 3)
CHECK_MISSING_BEATMAP_IDS = _get_bool("download", "check_missing_beatmap_ids", False)

API_REQUESTS_PER_MINUTE = _get_int("api", "requests_per_minute", 60)
API_RETRY_COUNT = _get_int("api", "retry_count", 3)
API_RETRY_DELAY = _get_float("api", "retry_delay", 0.5)
API_RATE_LIMIT = 0.0 if API_REQUESTS_PER_MINUTE <= 0 else 60.0 / API_REQUESTS_PER_MINUTE

LOG_LEVEL = SETTINGS.get("logging", "level", fallback="INFO")
OSU_API_LOG_LEVEL = SETTINGS.get("logging", "osu_api_level", fallback="INFO")

BACKEND_BASE_URL = SETTINGS.get(
    "oauth", "backend_base_url", fallback="https://api.lemon4ik.kz"
)
FRONTEND_BASE_URL = SETTINGS.get(
    "oauth", "frontend_base_url", fallback="https://lost.lemon4ik.kz"
)
OAUTH_CALLBACK_URL = f"{BACKEND_BASE_URL}/api/auth/callback"
API_PROXY_BASE = f"{BACKEND_BASE_URL}/api/proxy"


logger.info(
    "Configured API settings: API_REQUESTS_PER_MINUTE=%d, API_RETRY_COUNT=%s, API_RETRY_DELAY=%s, OSU_API_LOG_LEVEL=%s",
    API_REQUESTS_PER_MINUTE,
    API_RETRY_COUNT,
    API_RETRY_DELAY,
    OSU_API_LOG_LEVEL,
)
logger.info(
    "Backend OAuth configuration: BACKEND_BASE_URL=%s, FRONTEND_BASE_URL=%s, OAUTH_CALLBACK_URL=%s, API_PROXY_BASE=%s",
    BACKEND_BASE_URL,
    FRONTEND_BASE_URL,
    OAUTH_CALLBACK_URL,
    API_PROXY_BASE,
)
