import requests
import json
import re
import threading
import time
import logging
import functools
from config import CLIENT_ID, CLIENT_SECRET
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

api_lock = threading.Lock()
last_call = 0
session = requests.Session()

adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
session.mount("https://", adapter)
session.mount("http://", adapter)

TOKEN_CACHE = None

def wait_osu():
    global last_call
    with api_lock:
        now = time.time()
        diff = now - last_call
        if diff < 1/20:
            time.sleep((1/20) - diff)
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
                logger.warning(f"Retry {retries+1}/{max_retries} after error: {e}. Waiting {wait_time}s")
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
    logger.info("POST: %s", url)
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "public"
    }
    resp = session.post(url, data=data)
    resp.raise_for_status()
    TOKEN_CACHE = resp.json().get("access_token")
    return TOKEN_CACHE


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
            logger.error(f"Пользователь '{identifier}' (тип поиска: {lookup_key}) не найден.")
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP ошибка при запросе данных пользователя {identifier}: {e}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при запросе данных пользователя {identifier}: {e}")
        raise


@retry_request
def top_osu(token, user_id, limit=100):
    wait_osu()
    url = f"https://osu.ppy.sh/api/v2/users/{user_id}/scores/best?limit={limit}&include=beatmap"
    logger.info("GET top: %s", url)
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = session.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP ошибка при запросе топ-скоров пользователя {user_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при запросе топ-скоров пользователя {user_id}: {e}")
        raise


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
            logger.warning(f"Карта с ID {beatmap_id} не найдена")
            return None
        resp.raise_for_status()
        data = resp.json()

        if not data:
            logger.warning(f"Пустой ответ API для карты {beatmap_id}")
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
        logger.error(f"HTTP ошибка при запросе данных карты {beatmap_id}: {e}")
        if "429" in str(e):              
            time.sleep(5)                                                
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при запросе данных карты {beatmap_id}: {e}")
        raise


@retry_request
def lookup_osu(checksum):
    wait_osu()
    url = "https://osu.ppy.sh/api/v2/beatmaps/lookup"

    try:
        token = token_osu()
        if not token:
            logger.error("Не удалось получить токен для lookup_osu")
            return None

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        params = {"checksum": checksum}

        response = session.get(url, headers=headers, params=params)

        if response.status_code == 404:
            logger.warning(f"Карта с checksum {checksum} не найдена.")
            return None

        response.raise_for_status()
        data = response.json()

        if not data:
            logger.warning(f"Пустой ответ API для checksum {checksum}")
            return None

        return data.get("id")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP ошибка при поиске карты по checksum {checksum}: {e}")
        if "429" in str(e):              
            time.sleep(5)
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при поиске карты по checksum {checksum}: {e}")
        raise