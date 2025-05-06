import requests
import threading
import time
import os
import logging
import functools
import keyring
from utils import get_resource_path
from requests.adapters import HTTPAdapter
from config import API_RATE_LIMIT, API_RETRY_DELAY, API_RETRY_COUNT

# Использование единого логгера для модуля
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
TOKEN_CACHE_LOCK = threading.Lock()

MD5_TO_ID_CACHE = {}
MD5_TO_ID_CACHE_LOCK = threading.Lock()

IN_PROGRESS_LOOKUPS = {}
IN_PROGRESS_LOCK = threading.Lock()

CONFIG_DIR = get_resource_path("config")
USER_CONFIG_PATH = os.path.join(CONFIG_DIR, "api_keys.json")

from utils import get_env_path

ENV_PATH = os.environ.get("DOTENV_PATH")
if not ENV_PATH or not os.path.exists(ENV_PATH):
    ENV_PATH = get_env_path()


def wait_osu():
    global last_call
    with api_lock:
        now = time.time()
        diff = now - last_call

        # Check if rate limiting is enabled (API_RATE_LIMIT > 0)
        if API_RATE_LIMIT > 0 and diff < API_RATE_LIMIT:
            delay = API_RATE_LIMIT - diff
            logger.debug(f"Rate limiting: waiting {delay:.2f}s before next API call")
            time.sleep(delay)

        last_call = time.time()


def retry_request(func, max_retries=API_RETRY_COUNT, backoff_factor=API_RETRY_DELAY):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        logger.debug(
            f"API call to {func_name} with retry mechanism (max_retries={max_retries})"
        )

        retries = 0
        while retries < max_retries:
            try:
                logger.debug(
                    f"Executing {func_name} (attempt {retries + 1}/{max_retries + 1})"
                )
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                wait_time = backoff_factor * (2**retries)
                logger.warning(
                    f"Retry {retries + 1}/{max_retries} for {func_name} after error: {e}. Waiting {wait_time}s"
                )
                time.sleep(wait_time)
                retries += 1

        logger.warning(f"Last attempt for {func_name} after {max_retries} retries")
        return func(*args, **kwargs)

    return wrapper


def token_osu():
    global TOKEN_CACHE

    logger.debug("token_osu() called - checking cache")

    # Проверяем кэш с блокировкой
    with TOKEN_CACHE_LOCK:
        if TOKEN_CACHE is not None:
            logger.debug("Using cached TOKEN")
            return TOKEN_CACHE

    logger.info("TOKEN_CACHE miss - requesting new token")

    wait_osu()
    url = "https://osu.ppy.sh/oauth/token"

    client_id, client_secret = get_keys_from_keyring()

    if not client_id or not client_secret:
        logger.error("API keys not found in system keyring")
        return None

    logger.info("POST: %s with client: %s...", url, client_id[:3])

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "public",
    }

    try:
        logger.debug("Sending token request to osu! API")
        resp = session.post(url, data=data)

        if resp.status_code == 401:
            logger.error(
                "Invalid API credentials. Check your Client ID and Client Secret."
            )
            logger.error("Server response: %s", resp.text)
            return None

        resp.raise_for_status()
        token = resp.json().get("access_token")

        if token:
            logger.info("API token successfully received")

            # Сохраняем токен в кэш с блокировкой
            with TOKEN_CACHE_LOCK:
                TOKEN_CACHE = token

            return token
        else:
            logger.error("Token not received in API response")
            return None
    except Exception as e:
        logger.error("Error getting token: %s", e)
        return None


@retry_request
def user_osu(identifier, lookup_key, token):
    wait_osu()
    url = f"https://osu.ppy.sh/api/v2/users/{identifier}"
    params = {"key": lookup_key}
    logger.info("GET user: %s with params %s", url, params)
    headers = {"Authorization": f"Bearer {token}"}

    try:
        logger.debug(
            f"Sending request for user '{identifier}' (lookup type: {lookup_key})"
        )
        resp = session.get(url, headers=headers, params=params)

        if resp.status_code == 404:
            logger.error(
                "User '%s' (lookup type: %s) not found.", identifier, lookup_key
            )
            return None

        resp.raise_for_status()
        response_data = resp.json()
        logger.debug(
            f"Successfully retrieved user data for '{identifier}' (username: {response_data.get('username', 'unknown')})"
        )
        return response_data

    except requests.exceptions.HTTPError as e:
        logger.error("HTTP error when requesting user data %s: %s", identifier, e)
        raise
    except Exception as e:
        logger.error("Unexpected error when requesting user data %s: %s", identifier, e)
        raise


@retry_request
def top_osu(token, user_id, limit=200):
    all_scores = []
    page_size = 100
    logger.info(f"Retrieving top scores for user {user_id} (limit={limit})")

    for offset in range(0, limit, page_size):
        url = f"https://osu.ppy.sh/api/v2/users/{user_id}/scores/best"
        current_limit = min(page_size, limit - offset)

        logger.info(
            "GET top: %s (offset=%d, limit=%d)",
            url,
            offset,
            current_limit,
        )

        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "limit": current_limit,
            "offset": offset,
            "include": "beatmap",
        }

        wait_osu()
        try:
            logger.debug(
                f"Sending request for top scores (page {offset // page_size + 1})"
            )
            resp = session.get(url, headers=headers, params=params)
            resp.raise_for_status()
            page_scores = resp.json()

            if not page_scores:
                logger.info("No more scores found after offset %d", offset)
                break

            all_scores.extend(page_scores)
            logger.debug(
                "Retrieved %d scores (offset %d, total so far: %d)",
                len(page_scores),
                offset,
                len(all_scores),
            )

            if len(page_scores) < current_limit:
                logger.debug("Last page reached at offset %d", offset)
                break

        except requests.exceptions.HTTPError as e:
            logger.error(
                "HTTP error when requesting top scores for user %s: %s", user_id, e
            )
            raise
        except Exception as e:
            logger.error(
                "Unexpected error when requesting top scores for user %s: %s",
                user_id,
                e,
            )
            raise

    logger.info(f"Total of {len(all_scores)} scores retrieved for user {user_id}")
    return all_scores


@retry_request
def map_osu(beatmap_id, token):
    if not beatmap_id:
        logger.warning("map_osu called with empty beatmap_id")
        return None

    wait_osu()
    url = f"https://osu.ppy.sh/api/v2/beatmaps/{beatmap_id}"
    logger.info("GET map: %s", url)
    headers = {"Authorization": f"Bearer {token}"}

    try:
        logger.debug(f"Sending request for beatmap {beatmap_id}")
        resp = session.get(url, headers=headers)

        if resp.status_code == 404:
            logger.warning("Beatmap with ID %s not found", beatmap_id)
            # Return a minimal info structure with not_found status
            return {
                "status": "not_found",
                "artist": "",
                "title": f"Not Found (ID: {beatmap_id})",
                "version": "",
                "creator": "",
                "hit_objects": 0,
            }

        resp.raise_for_status()
        data = resp.json()

        if not data:
            logger.warning("Empty API response for beatmap %s", beatmap_id)
            return None

        bset = data.get("beatmapset", {})

        c = data.get("count_circles", 0)
        s = data.get("count_sliders", 0)
        sp = data.get("count_spinners", 0)
        hobj = c + s + sp

        # Полная информация, включая данные об обложке
        result = {
            "id": beatmap_id,
            "status": data.get("status", "unknown"),
            "artist": bset.get("artist", ""),
            "title": bset.get("title", ""),
            "version": data.get("version", ""),
            "creator": bset.get("creator", ""),
            "hit_objects": hobj,
            "beatmapset": bset,  # Добавляем полную информацию о beatmapset, которая включает обложки
        }

        logger.debug(
            f"Successfully retrieved beatmap {beatmap_id}: {result['artist']} - {result['title']} [{result['version']}], status: {result['status']}"
        )
        return result

    except requests.exceptions.HTTPError as e:
        logger.error("HTTP error when requesting beatmap data %s: %s", beatmap_id, e)
        if "429" in str(e):
            logger.warning("Rate limit hit (429), sleeping for 5 seconds")
            time.sleep(5)
        raise
    except Exception as e:
        logger.error(
            "Unexpected error when requesting beatmap data %s: %s", beatmap_id, e
        )
        raise


@retry_request
def lookup_osu(checksum):
    # Первая проверка - есть ли значение в кэше (быстрая проверка без длительной блокировки)
    with MD5_TO_ID_CACHE_LOCK:
        if checksum in MD5_TO_ID_CACHE:
            cached_id = MD5_TO_ID_CACHE[checksum]
            logger.info(f"Using cached beatmap_id {cached_id} for checksum {checksum}")

            # Если ID есть в кэше, но нам нужна полная информация о карте
            if cached_id is not None:
                # Здесь нам нужно получить информацию о карте через map_osu
                try:
                    token = token_osu()
                    if token:
                        beatmap_data = map_osu(cached_id, token)
                        if beatmap_data:
                            return beatmap_data
                except Exception as e:
                    logger.error(
                        f"Error getting beatmap data for cached ID {cached_id}: {e}"
                    )

            return cached_id

    # Проверяем, не выполняется ли уже запрос для этого MD5
    wait_event = None
    with IN_PROGRESS_LOCK:
        # Если запрос для этого MD5 уже выполняется, будем ждать его завершения
        if checksum in IN_PROGRESS_LOOKUPS:
            wait_event = IN_PROGRESS_LOOKUPS[checksum]["event"]
            IN_PROGRESS_LOOKUPS[checksum]["waiters"] += 1
            logger.debug(
                f"Waiting for in-progress lookup of checksum {checksum}, now has {IN_PROGRESS_LOOKUPS[checksum]['waiters']} waiters"
            )
        else:
            # Если запроса нет, создаем запись и продолжаем сами
            wait_event = threading.Event()
            IN_PROGRESS_LOOKUPS[checksum] = {
                "event": wait_event,
                "waiters": 0,
                "result": None,
            }
            logger.debug(f"Starting new lookup for checksum {checksum}")

    # Если мы должны ждать другой поток
    if wait_event and IN_PROGRESS_LOOKUPS[checksum]["waiters"] > 0:
        logger.debug(f"Waiting for completion of checksum {checksum} lookup")
        wait_event.wait()  # Ждем завершения другого потока

        # После ожидания проверяем результат и освобождаем ресурсы
        with IN_PROGRESS_LOCK:
            result = IN_PROGRESS_LOOKUPS[checksum]["result"]
            IN_PROGRESS_LOOKUPS[checksum]["waiters"] -= 1

            # Если больше нет ожидающих, удаляем запись
            if IN_PROGRESS_LOOKUPS[checksum]["waiters"] <= 0:
                # Только если мы последний ждущий поток, удаляем запись
                if (
                    checksum in IN_PROGRESS_LOOKUPS
                    and IN_PROGRESS_LOOKUPS[checksum]["waiters"] == 0
                ):
                    del IN_PROGRESS_LOOKUPS[checksum]
                    logger.debug(f"Removed in-progress entry for checksum {checksum}")

        logger.debug(
            f"Returning cached result for checksum {checksum} after waiting: {result}"
        )
        return result

    # Если мы здесь, значит мы основной поток, который будет выполнять запрос
    # Если мы здесь, значит мы основной поток, который будет выполнять запрос
    try:
        # Повторная проверка кэша (чтобы избежать гонки условий после ожидания)
        with MD5_TO_ID_CACHE_LOCK:
            if checksum in MD5_TO_ID_CACHE:
                cached_id = MD5_TO_ID_CACHE[checksum]
                logger.info(
                    f"Using cached beatmap_id {cached_id} for checksum {checksum} (after recheck)"
                )

                # Если ID есть в кэше, но нам нужна полная информация о карте
                full_data = None
                if cached_id is not None:
                    try:
                        token = token_osu()
                        if token:
                            full_data = map_osu(cached_id, token)
                    except Exception as e:
                        logger.error(
                            f"Error getting beatmap data for cached ID {cached_id}: {e}"
                        )

                # Устанавливаем результат для ожидающих потоков
                with IN_PROGRESS_LOCK:
                    if checksum in IN_PROGRESS_LOOKUPS:
                        IN_PROGRESS_LOOKUPS[checksum]["result"] = (
                            full_data if full_data else cached_id
                        )
                        IN_PROGRESS_LOOKUPS[checksum][
                            "event"
                        ].set()  # Сигнализируем ожидающим потокам

                return full_data if full_data else cached_id

        # Теперь выполняем API-запрос
        wait_osu()
        url = "https://osu.ppy.sh/api/v2/beatmaps/lookup"

        token = token_osu()
        if not token:
            logger.error("Failed to get token for lookup_osu")
            result = None

            # Устанавливаем результат для ожидающих потоков
            with IN_PROGRESS_LOCK:
                if checksum in IN_PROGRESS_LOOKUPS:
                    IN_PROGRESS_LOOKUPS[checksum]["result"] = result
                    IN_PROGRESS_LOOKUPS[checksum]["event"].set()

            return result

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        params = {"checksum": checksum}

        response = session.get(url, headers=headers, params=params)

        if response.status_code == 404:
            logger.warning("Beatmap with checksum %s not found.", checksum)
            # Кэшируем отрицательный результат
            with MD5_TO_ID_CACHE_LOCK:
                MD5_TO_ID_CACHE[checksum] = None
            result = None

            # Устанавливаем результат для ожидающих потоков
            with IN_PROGRESS_LOCK:
                if checksum in IN_PROGRESS_LOOKUPS:
                    IN_PROGRESS_LOOKUPS[checksum]["result"] = result
                    IN_PROGRESS_LOOKUPS[checksum]["event"].set()

            return result

        response.raise_for_status()
        data = response.json()

        if not data:
            logger.warning("Empty API response for checksum %s", checksum)
            result = None

            # Устанавливаем результат для ожидающих потоков
            with IN_PROGRESS_LOCK:
                if checksum in IN_PROGRESS_LOOKUPS:
                    IN_PROGRESS_LOOKUPS[checksum]["result"] = result
                    IN_PROGRESS_LOOKUPS[checksum]["event"].set()

            return result

        beatmap_id = data.get("id")

        # Кэшируем только ID в кэше
        if beatmap_id:
            with MD5_TO_ID_CACHE_LOCK:
                MD5_TO_ID_CACHE[checksum] = beatmap_id
                logger.info(f"Cached beatmap_id {beatmap_id} for checksum {checksum}")

        # Преобразуем полученные данные в нужный формат
        bset = data.get("beatmapset", {})

        # Подсчитываем общее количество игровых объектов
        c = data.get("count_circles", 0)
        s = data.get("count_sliders", 0)
        sp = data.get("count_spinners", 0)
        hobj = c + s + sp

        result = {
            "id": beatmap_id,
            "status": data.get("status", "unknown"),
            "artist": bset.get("artist", ""),
            "title": bset.get("title", ""),
            "version": data.get("version", ""),
            "creator": bset.get("creator", ""),
            "hit_objects": hobj,
            "count_circles": c,
            "count_sliders": s,
            "count_spinners": sp,
        }

        # Устанавливаем результат для ожидающих потоков (полные данные)
        with IN_PROGRESS_LOCK:
            if checksum in IN_PROGRESS_LOOKUPS:
                IN_PROGRESS_LOOKUPS[checksum]["result"] = result
                IN_PROGRESS_LOOKUPS[checksum]["event"].set()

        return result

    except Exception as e:
        logger.error("Error when looking up beatmap by checksum %s: %s", checksum, e)

        # В случае ошибки тоже нужно сообщить ожидающим потокам
        with IN_PROGRESS_LOCK:
            if checksum in IN_PROGRESS_LOOKUPS:
                IN_PROGRESS_LOOKUPS[checksum]["result"] = None
                IN_PROGRESS_LOOKUPS[checksum]["event"].set()

        # Пробрасываем исключение для обработки декоратором retry_request
        raise

    finally:
        # Если мы единственный поток и нет ожидающих, очищаем запись
        with IN_PROGRESS_LOCK:
            if (
                checksum in IN_PROGRESS_LOOKUPS
                and IN_PROGRESS_LOOKUPS[checksum]["waiters"] == 0
            ):
                del IN_PROGRESS_LOOKUPS[checksum]
                logger.debug(
                    f"Cleanup: removed in-progress entry for checksum {checksum}"
                )


def reset_api_caches():
    """Reset all in-memory caches in osu_api module"""
    global TOKEN_CACHE, MD5_TO_ID_CACHE

    with TOKEN_CACHE_LOCK:
        TOKEN_CACHE = None

    with MD5_TO_ID_CACHE_LOCK:
        cache_size = len(MD5_TO_ID_CACHE)
        MD5_TO_ID_CACHE.clear()

    logger.info(
        f"All osu_api caches have been reset (cleared {cache_size} MD5-to-ID mappings)"
    )


def save_keys_to_keyring(client_id, client_secret):
    try:
        if client_id and client_secret:
            keyring.set_password(KEYRING_SERVICE, CLIENT_ID_KEY, client_id)
            keyring.set_password(KEYRING_SERVICE, CLIENT_SECRET_KEY, client_secret)
            logger.info(
                "API keys saved to system keyring (CLIENT_ID: %s...)", client_id[:3]
            )
            return True
        else:
            logger.warning("Cannot save empty API keys")
            return False
    except Exception as e:
        logger.error("Error saving API keys to keyring: %s", e)
        return False


def get_keys_from_keyring():
    try:
        client_id = keyring.get_password(KEYRING_SERVICE, CLIENT_ID_KEY)
        client_secret = keyring.get_password(KEYRING_SERVICE, CLIENT_SECRET_KEY)

        if client_id and client_secret:
            logger.info(
                "API keys retrieved from system keyring (CLIENT_ID: %s...)",
                client_id[:3],
            )
        else:
            logger.warning("API keys not found in system keyring")

        return client_id, client_secret
    except Exception as e:
        logger.error("Error retrieving API keys from keyring: %s", e)
        return None, None


def delete_keys_from_keyring():
    try:
        keyring.delete_password(KEYRING_SERVICE, CLIENT_ID_KEY)
        keyring.delete_password(KEYRING_SERVICE, CLIENT_SECRET_KEY)
        logger.info("API keys deleted from system keyring")
        return True
    except Exception as e:
        logger.error("Error deleting API keys from keyring: %s", e)
        return False
