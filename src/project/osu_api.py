import requests
import json
import re
import threading
import time
import os
import logging
import functools
from config import CLIENT_ID, CLIENT_SECRET
from utils import get_resource_path
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

api_lock = threading.Lock()
last_call = 0
session = requests.Session()

adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
session.mount("https://", adapter)
session.mount("http://", adapter)

TOKEN_CACHE = None

CONFIG_DIR = get_resource_path("config")
USER_CONFIG_PATH = os.path.join(CONFIG_DIR, "api_keys.json")

ENV_PATH = os.environ.get("DOTENV_PATH")
if not ENV_PATH or not os.path.exists(ENV_PATH):
    ENV_PATH = get_resource_path(os.path.join("..", ".env"))


def wait_osu():
    global last_call
    with api_lock:
        now = time.time()
        diff = now - last_call
        if diff < 1 / 20:
            time.sleep((1 / 20) - diff)
        last_call = time.time()


def retry_request(func, max_retries=3, backoff_factor=0.5):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        retries = 0
        while retries < max_retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                wait_time = backoff_factor * (2 ** retries)
                logger.warning(f"Retry {retries + 1}/{max_retries} after error: {e}. Waiting {wait_time}s")
                time.sleep(wait_time)
                retries += 1

        return func(*args, **kwargs)

    return wrapper


def token_osu():
    global TOKEN_CACHE
    if TOKEN_CACHE is not None:
        return TOKEN_CACHE
    wait_osu()
    url = "https://osu.ppy.sh/oauth/token"

    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")

    logger.info(f"POST: {url} with client: {client_id[:4]}...")

    if client_id == "default_client_id" or client_secret == "default_client_secret":
        logger.error("Default values are being used for CLIENT_ID or CLIENT_SECRET")
        logger.error(f"CLIENT_ID: {client_id[:4]}..., CLIENT_SECRET: {client_secret[:4]}...")
        return None

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "public"
    }

    try:
        resp = session.post(url, data=data)
        if resp.status_code == 401:
            logger.error(f"Invalid API credentials. Check your CLIENT_ID and CLIENT_SECRET.")
            logger.error(f"Server response: {resp.text}")
            return None

        resp.raise_for_status()
        token = resp.json().get("access_token")
        if token:
            logger.info("API token successfully received")
            TOKEN_CACHE = token
            return token
        else:
            logger.error("Token not received in API response")
            return None
    except Exception as e:
        logger.error(f"Error getting token: {e}")
        return None


@retry_request
def user_osu(identifier, lookup_key, token):
    wait_osu()
    url = f"https://osu.ppy.sh/api/v2/users/{identifier}"
    params = {
        'key': lookup_key
    }
    logger.info("GET user: %s with params %s", url, params)
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = session.get(url, headers=headers, params=params)
        if resp.status_code == 404:
            logger.error(f"User '{identifier}' (lookup type: {lookup_key}) not found.")
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error when requesting user data {identifier}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error when requesting user data {identifier}: {e}")
        raise


@retry_request
def top_osu(token, user_id, limit=200):
    all_scores = []
    page_size = 100

    for offset in range(0, limit, page_size):
        url = f"https://osu.ppy.sh/api/v2/users/{user_id}/scores/best"
        logger.info(f"GET top: {url} (offset={offset}, limit={min(page_size, limit - offset)})")
        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "limit": min(page_size, limit - offset),
            "offset": offset,
            "include": "beatmap"
        }

        wait_osu()
        try:
            resp = session.get(url, headers=headers, params=params)
            resp.raise_for_status()
            page_scores = resp.json()

            if not page_scores:
                logger.info(f"No more scores found after offset {offset}")
                break

            all_scores.extend(page_scores)
            logger.debug(f"Retrieved {len(page_scores)} scores (offset {offset})")

            if len(page_scores) < min(page_size, limit - offset):
                logger.debug(f"Last page reached at offset {offset}")
                break

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error when requesting top scores for user {user_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error when requesting top scores for user {user_id}: {e}")
            raise

    return all_scores


@retry_request
def map_osu(beatmap_id, token):
    if not beatmap_id:
        return None
    wait_osu()
    url = f"https://osu.ppy.sh/api/v2/beatmaps/{beatmap_id}"
    logger.info("GET map: %s", url)
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = session.get(url, headers=headers)
        if resp.status_code == 404:
            logger.warning(f"Beatmap with ID {beatmap_id} not found")
            return None
        resp.raise_for_status()
        data = resp.json()

        if not data:
            logger.warning(f"Empty API response for beatmap {beatmap_id}")
            return None

        bset = data.get("beatmapset", {})

        c = data.get("count_circles", 0)
        s = data.get("count_sliders", 0)
        sp = data.get("count_spinners", 0)
        hobj = c + s + sp

        return {
            "status": data.get("status", "unknown"),
            "artist": bset.get("artist", ""),
            "title": bset.get("title", ""),
            "version": data.get("version", ""),
            "creator": bset.get("creator", ""),
            "hit_objects": hobj
        }
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error when requesting beatmap data {beatmap_id}: {e}")
        if "429" in str(e):
            time.sleep(5)
        raise
    except Exception as e:
        logger.error(f"Unexpected error when requesting beatmap data {beatmap_id}: {e}")
        raise


@retry_request
def lookup_osu(checksum):
    wait_osu()
    url = "https://osu.ppy.sh/api/v2/beatmaps/lookup"

    try:
        token = token_osu()
        if not token:
            logger.error("Failed to get token for lookup_osu")
            return None

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        params = {"checksum": checksum}

        response = session.get(url, headers=headers, params=params)

        if response.status_code == 404:
            logger.warning(f"Beatmap with checksum {checksum} not found.")
            return None

        response.raise_for_status()
        data = response.json()

        if not data:
            logger.warning(f"Empty API response for checksum {checksum}")
            return None

        return data.get("id")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error when looking up beatmap by checksum {checksum}: {e}")
        if "429" in str(e):
            time.sleep(5)
        raise
    except Exception as e:
        logger.error(f"Unexpected error when looking up beatmap by checksum {checksum}: {e}")
        raise


def save_api_keys(client_id, client_secret):
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)

        logger.info(f"Saving API keys to: {USER_CONFIG_PATH}")
        logger.info(f"CLIENT_ID: {client_id[:4]}..., CLIENT_SECRET: {client_secret[:4]}...")

        with open(USER_CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump({
                "client_id": client_id,
                "client_secret": client_secret
            }, f, indent=4)
        return True
    except Exception as e:
        logger.error(f"Error saving API keys: {e}")
        return False


def load_api_keys():
    if not os.path.exists(USER_CONFIG_PATH):
        return None, None

    try:
        with open(USER_CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config.get("client_id"), config.get("client_secret")
    except Exception as e:
        logger.error(f"Error loading API keys: {e}")
        return None, None


def update_env_file(client_id, client_secret):
    try:
        logger.info(f"Updating .env file: {ENV_PATH}")

        if not os.path.exists(ENV_PATH):
            logger.warning(f".env file not found, creating new: {ENV_PATH}")
            with open(ENV_PATH, 'w', encoding='utf-8') as f:
                f.write("CLIENT_ID=default_client_id\n")
                f.write("CLIENT_SECRET=default_client_secret\n")
                f.write("DB_FILE=../cache/beatmap_info.db\n")
                f.write("CUTOFF_DATE=1719619200\n")

        current_env = {}
        with open(ENV_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    current_env[key] = value

        current_env['CLIENT_ID'] = client_id
        current_env['CLIENT_SECRET'] = client_secret

        if 'DB_FILE' not in current_env:
            current_env['DB_FILE'] = '../cache/beatmap_info.db'
        if 'CUTOFF_DATE' not in current_env:
            current_env['CUTOFF_DATE'] = '1719619200'

        env_content = []
        for key, value in current_env.items():
            env_content.append(f'{key}={value}')

        with open(ENV_PATH, 'w', encoding='utf-8') as f:
            f.write('\n'.join(env_content))

        os.environ["CLIENT_ID"] = client_id
        os.environ["CLIENT_SECRET"] = client_secret

        logger.info(
            f"Environment variables updated: CLIENT_ID={os.environ.get('CLIENT_ID')[:4]}..., CLIENT_SECRET={os.environ.get('CLIENT_SECRET')[:4]}...")

        try:
            from dotenv import load_dotenv
            load_dotenv(dotenv_path=ENV_PATH, override=True)
            logger.info(".env file reload completed successfully")
        except Exception as dotenv_error:
            logger.warning(f"Failed to reload .env file: {dotenv_error}")

        logger.info(f".env file updated at path: {ENV_PATH}")
        return True
    except Exception as e:
        logger.error(f"Error updating .env file: {e}")
        return False


def restore_env_defaults():
    try:
        logger.info(f"Restoring .env file to default values: {ENV_PATH}")

        if not os.path.exists(ENV_PATH):
            logger.warning(f".env file not found, creating new: {ENV_PATH}")
            with open(ENV_PATH, 'w', encoding='utf-8') as f:
                f.write("CLIENT_ID=default_client_id\n")
                f.write("CLIENT_SECRET=default_client_secret\n")
                f.write("DB_FILE=../cache/beatmap_info.db\n")
                f.write("CUTOFF_DATE=1719619200\n")
            return True

        current_env = {}
        with open(ENV_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    current_env[key] = value

        current_env['CLIENT_ID'] = 'default_client_id'
        current_env['CLIENT_SECRET'] = 'default_client_secret'

        env_content = []
        for key, value in current_env.items():
            env_content.append(f'{key}={value}')

        with open(ENV_PATH, 'w', encoding='utf-8') as f:
            f.write('\n'.join(env_content))

        logger.info(f".env file restored to default values: {ENV_PATH}")
        return True
    except Exception as e:
        logger.error(f"Error restoring .env file: {e}")
        return False


def setup_api_keys():
    client_id, client_secret = load_api_keys()

    if not client_id or not client_secret:

        logger.warning("API keys not found. Input required through interface.")
        return False
    else:
        logger.info(f"Using saved API keys: {client_id[:4]}...")

    result = update_env_file(client_id, client_secret)
    return result
