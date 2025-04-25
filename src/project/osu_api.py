import requests
import threading
import time
import os
import logging
import functools
import keyring
from utils import get_resource_path
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

api_lock = threading.Lock()
last_call = 0
session = requests.Session()

adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
session.mount("https://", adapter)
session.mount("http://", adapter)

KEYRING_SERVICE = "osu_lost_scores_analyzer"
CLIENT_ID_KEY = "client_id"
CLIENT_SECRET_KEY = "client_secret"

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
        if diff < 1:
            time.sleep((1) - diff)
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

    client_id, client_secret = get_keys_from_keyring()

    if not client_id or not client_secret:
        logger.error("API keys not found in system keyring")
        return None

    logger.info(f"POST: {url} with client: {client_id[:4]}...")

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "public"
    }

    try:
        resp = session.post(url, data=data)
        if resp.status_code == 401:
            logger.error(f"Invalid API credentials. Check your Client ID and Client Secret.")
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


def save_keys_to_keyring(client_id, client_secret):
                                                                   
    try:
        if client_id and client_secret:
            keyring.set_password(KEYRING_SERVICE, CLIENT_ID_KEY, client_id)
            keyring.set_password(KEYRING_SERVICE, CLIENT_SECRET_KEY, client_secret)
            logger.info(f"API keys saved to system keyring (CLIENT_ID: {client_id[:4]}...)")
            return True
        else:
            logger.warning("Cannot save empty API keys")
            return False
    except Exception as e:
        logger.error(f"Error saving API keys to keyring: {e}")
        return False


def get_keys_from_keyring():
                                                                    
    try:
        client_id = keyring.get_password(KEYRING_SERVICE, CLIENT_ID_KEY)
        client_secret = keyring.get_password(KEYRING_SERVICE, CLIENT_SECRET_KEY)

        if client_id and client_secret:
            logger.info(f"API keys retrieved from system keyring (CLIENT_ID: {client_id[:4]}...)")
        else:
            logger.warning("API keys not found in system keyring")

        return client_id, client_secret
    except Exception as e:
        logger.error(f"Error retrieving API keys from keyring: {e}")
        return None, None


def delete_keys_from_keyring():
                                                                   
    try:
        keyring.delete_password(KEYRING_SERVICE, CLIENT_ID_KEY)
        keyring.delete_password(KEYRING_SERVICE, CLIENT_SECRET_KEY)
        logger.info("API keys deleted from system keyring")
        return True
    except Exception as e:
        logger.error(f"Error deleting API keys from keyring: {e}")
        return False
