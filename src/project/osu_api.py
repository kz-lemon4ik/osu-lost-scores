import requests
import json
import re
import threading
import time
import os
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

                                           
CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src", "config")
USER_CONFIG_PATH = os.path.join(CONFIG_DIR, "api_keys.json")

                                                    
ENV_PATH = os.environ.get("DOTENV_PATH")
if not ENV_PATH or not os.path.exists(ENV_PATH):
                                                                                    
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ENV_PATH = os.path.join(project_root, ".env")
    ENV_PATH = os.path.abspath(ENV_PATH)
    logger.warning(f"DOTENV_PATH не задан или файл не существует, используем: {ENV_PATH}")

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

                                                                                   
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")

    logger.info(f"POST: {url} с клиентом: {client_id[:4]}...")

    if client_id == "default_client_id" or client_secret == "default_client_secret":
        logger.error("Используются значения по умолчанию для CLIENT_ID или CLIENT_SECRET")
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
            logger.error(f"Неверные учетные данные API. Проверьте ваши CLIENT_ID и CLIENT_SECRET.")
            logger.error(f"Ответ сервера: {resp.text}")
            return None

        resp.raise_for_status()
        token = resp.json().get("access_token")
        if token:
            logger.info("Успешно получен токен API")
            TOKEN_CACHE = token
            return token
        else:
            logger.error("Токен не получен в ответе API")
            return None
    except Exception as e:
        logger.error(f"Ошибка при получении токена: {e}")
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
            logger.error(f"HTTP ошибка при запросе топ-скоров пользователя {user_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при запросе топ-скоров пользователя {user_id}: {e}")
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


def save_api_keys(client_id, client_secret):
                                                     
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)

        logger.info(f"Сохраняем API ключи в: {USER_CONFIG_PATH}")
        logger.info(f"CLIENT_ID: {client_id[:4]}..., CLIENT_SECRET: {client_secret[:4]}...")

        with open(USER_CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump({
                "client_id": client_id,
                "client_secret": client_secret
            }, f, indent=4)
        return True
    except Exception as e:
        logger.error(f"Ошибка при сохранении API ключей: {e}")
        return False

def load_api_keys():
                                                        
    if not os.path.exists(USER_CONFIG_PATH):
        return None, None

    try:
        with open(USER_CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config.get("client_id"), config.get("client_secret")
    except Exception as e:
        logger.error(f"Ошибка при загрузке API ключей: {e}")
        return None, None


def update_env_file(client_id, client_secret):
                                                                         
    try:
        logger.info(f"Обновляем .env файл: {ENV_PATH}")

                                            
        if not os.path.exists(ENV_PATH):
            logger.warning(f".env файл не найден, создаем новый: {ENV_PATH}")
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
            f"Переменные окружения обновлены: CLIENT_ID={os.environ.get('CLIENT_ID')[:4]}..., CLIENT_SECRET={os.environ.get('CLIENT_SECRET')[:4]}...")

                                        
        try:
            from dotenv import load_dotenv
            load_dotenv(dotenv_path=ENV_PATH, override=True)
            logger.info("Перезагрузка .env файла выполнена успешно")
        except Exception as dotenv_error:
            logger.warning(f"Не удалось перезагрузить .env файл: {dotenv_error}")

        logger.info(f".env файл обновлен по пути: {ENV_PATH}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при обновлении .env файла: {e}")
        return False


def restore_env_defaults():
                                                            
    try:
        logger.info(f"Восстанавливаем .env файл до значений по умолчанию: {ENV_PATH}")

                                            
        if not os.path.exists(ENV_PATH):
            logger.warning(f".env файл не найден, создаем новый: {ENV_PATH}")
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

        logger.info(f".env файл восстановлен до значений по умолчанию: {ENV_PATH}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при восстановлении .env файла: {e}")
        return False


def setup_api_keys():
                                                     
                                         
    global os

    client_id, client_secret = load_api_keys()

    if not client_id or not client_secret:
                                                         
        env_client_id = os.environ.get("OSU_CLIENT_ID")
        env_client_secret = os.environ.get("OSU_CLIENT_SECRET")

        if env_client_id and env_client_secret:
            client_id = env_client_id
            client_secret = env_client_secret
            logger.info("Используются API ключи из переменных окружения")
            logger.info(f"CLIENT_ID: {client_id[:4]}...")
            save_api_keys(client_id, client_secret)
        else:
            logger.error("API ключи не найдены и не переданы через переменные окружения")
            print("Ошибка: API ключи osu! не найдены.")
            print("Запустите программу с параметрами OSU_CLIENT_ID и OSU_CLIENT_SECRET.")
            print("Пример: OSU_CLIENT_ID=123 OSU_CLIENT_SECRET=abc python main.py")
            return False
    else:
        logger.info(f"Используются сохраненные API ключи: {client_id[:4]}...")

                                                
    result = update_env_file(client_id, client_secret)

                                                       
    if result:
        logger.info(f"Проверка обновления окружения: CLIENT_ID={os.environ.get('CLIENT_ID')[:4]}...")
    else:
        logger.error("Не удалось обновить .env файл")

    return result