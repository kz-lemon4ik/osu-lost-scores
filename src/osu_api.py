import requests
import threading
import time
import os
import logging
import functools
import json
import keyring
from utils import mask_path_for_log, get_env_path
from requests.adapters import HTTPAdapter

                                          
logger = logging.getLogger(__name__)

KEYRING_SERVICE = "osu_lost_scores_analyzer"
CLIENT_ID_KEY = "client_id"
CLIENT_SECRET_KEY = "client_secret"

ENV_PATH = os.environ.get("DOTENV_PATH")
if not ENV_PATH or not os.path.exists(ENV_PATH):
    ENV_PATH = get_env_path()


class OsuApiClient:
    def __init__(
        self,
        client_id,
        client_secret,
        token_cache_path=None,
        md5_cache_path=None,
        api_rate_limit=1.0,
        api_retry_count=3,
        api_retry_delay=0.5,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_rate_limit = api_rate_limit
        self.api_retry_count = api_retry_count
        self.api_retry_delay = api_retry_delay

                             
        self.token_cache_path = token_cache_path
        self.md5_cache_path = md5_cache_path

                        
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

                                   
        self.api_lock = threading.Lock()
        self.last_call = 0

        self.token_cache = None
        self.token_cache_lock = threading.Lock()

        self.md5_to_id_cache = {}
        self.md5_to_id_cache_lock = threading.Lock()

        self.in_progress_lookups = {}
        self.in_progress_lock = threading.Lock()

                                          
        self._load_token_from_file()
        self._load_md5_cache_from_file()

    _instance = None

    @classmethod
    def get_instance(cls, client_id=None, client_secret=None, token_cache_path=None,
                     md5_cache_path=None, api_rate_limit=1.0, api_retry_count=3,
                     api_retry_delay=0.5):
                   
        if cls._instance is None:
                                                                    
                                                       
            if not client_id or not client_secret:
                client_id, client_secret = cls.get_keys_from_keyring()

                                                
            if client_id and client_secret:
                cls._instance = cls(
                    client_id=client_id,
                    client_secret=client_secret,
                    token_cache_path=token_cache_path,
                    md5_cache_path=md5_cache_path,
                    api_rate_limit=api_rate_limit,
                    api_retry_count=api_retry_count,
                    api_retry_delay=api_retry_delay
                )
        elif client_id and client_secret:
                                                                                      
            cls._instance.client_id = client_id
            cls._instance.client_secret = client_secret
                                                                          
            with cls._instance.token_cache_lock:
                cls._instance.token_cache = None

        return cls._instance

    @classmethod
    def reset_instance(cls):
                   
        cls._instance = None

    def _load_token_from_file(self):
                                      
        if not self.token_cache_path:
            return

        try:
            if os.path.exists(self.token_cache_path):
                with open(self.token_cache_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    with self.token_cache_lock:
                        self.token_cache = data.get("token")
                    logger.debug(f"Token loaded from file: {self.token_cache_path}")
        except (FileNotFoundError, json.JSONDecodeError, PermissionError) as e:
            logger.warning(f"Failed to load token from file: {e}")

    def _save_token_to_file(self):
                                    
        if not self.token_cache_path or not self.token_cache:
            return

        try:
                                                                  
            os.makedirs(
                os.path.dirname(os.path.abspath(self.token_cache_path)), exist_ok=True
            )

            with open(self.token_cache_path, "w", encoding="utf-8") as f:
                json.dump({"token": self.token_cache}, f, indent=2)
            logger.debug(f"Token saved to file: {self.token_cache_path}")
        except (FileNotFoundError, PermissionError) as e:
            logger.warning(f"Failed to save token to file: {e}")

    def _load_md5_cache_from_file(self):
                                            
        if not self.md5_cache_path:
            return

        try:
            if os.path.exists(self.md5_cache_path):
                with open(self.md5_cache_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    with self.md5_to_id_cache_lock:
                        self.md5_to_id_cache = data
                    logger.info(
                        f"MD5 cache loaded from file: {self.md5_cache_path} ({len(self.md5_to_id_cache)} entries)"
                    )
        except (FileNotFoundError, json.JSONDecodeError, PermissionError) as e:
            logger.warning(f"Failed to load MD5 cache from file: {e}")

    def _save_md5_cache_to_file(self):
                                          
        if not self.md5_cache_path:
            return

        try:
                                                                  
            os.makedirs(
                os.path.dirname(os.path.abspath(self.md5_cache_path)), exist_ok=True
            )

            with self.md5_to_id_cache_lock:
                cache_copy = dict(
                    self.md5_to_id_cache
                )                                                           

            with open(self.md5_cache_path, "w", encoding="utf-8") as f:
                json.dump(cache_copy, f, indent=2)
            logger.debug(
                f"MD5 cache saved to file: {self.md5_cache_path} ({len(cache_copy)} entries)"
            )
        except (FileNotFoundError, PermissionError) as e:
            logger.warning(f"Failed to save MD5 cache to file: {e}")

    def _wait_for_api_slot(self):
                                                                                                    
        with self.api_lock:
            now = time.time()
            diff = now - self.last_call

                                                                    
            if self.api_rate_limit > 0 and diff < self.api_rate_limit:
                delay = self.api_rate_limit - diff
                logger.debug(
                    f"Rate limiting: waiting {delay:.2f}s before next API call"
                )
                time.sleep(delay)

            self.last_call = time.time()

    def _retry_request(self, func):
                                                                                          

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            logger.debug(
                f"API call to {func_name} with retry mechanism (max_retries={self.api_retry_count})"
            )

            retries = 0
            while retries < self.api_retry_count:
                try:
                    logger.debug(
                        f"Executing {func_name} (attempt {retries + 1}/{self.api_retry_count + 1})"
                    )
                    response = func(*args, **kwargs)
                    return response
                except requests.exceptions.HTTPError as e:
                    status_code = e.response.status_code if hasattr(e, 'response') else None

                                                         
                    if status_code == 401:                                     
                        logger.error(f"Authentication error (401) in {func_name}: {e}")
                        with self.token_cache_lock:
                            self.token_cache = None
                        logger.info("Token invalidated due to 401 error")
                                                             
                        raise

                    elif status_code == 404:             
                        logger.warning(f"Resource not found (404) in {func_name}: {e}")
                                                             
                        raise

                    elif status_code == 429:                     
                                                                   
                        wait_time = self.api_retry_delay * (4 ** retries)
                        logger.warning(
                            f"Rate limit exceeded (429) in {func_name}. Waiting {wait_time}s before retry"
                        )
                        time.sleep(wait_time)
                        retries += 1
                        continue

                                                                                 
                    wait_time = self.api_retry_delay * (2 ** retries)
                    logger.warning(
                        f"HTTP error in {func_name} (status={status_code}): {e}. Retry {retries + 1}/{self.api_retry_count} after {wait_time}s"
                    )
                    time.sleep(wait_time)
                    retries += 1

                except requests.exceptions.ConnectionError as e:
                                                                         
                    wait_time = self.api_retry_delay * (3 ** retries)
                    logger.warning(
                        f"Connection error in {func_name}: {e}. Retry {retries + 1}/{self.api_retry_count} after {wait_time}s"
                    )
                    time.sleep(wait_time)
                    retries += 1

                except requests.exceptions.RequestException as e:
                                            
                    wait_time = self.api_retry_delay * (2 ** retries)
                    logger.warning(
                        f"Request error in {func_name}: {e}. Retry {retries + 1}/{self.api_retry_count} after {wait_time}s"
                    )
                    time.sleep(wait_time)
                    retries += 1

                except Exception as e:
                                                                          
                    logger.error(f"Unexpected error in {func_name}: {e}")
                    raise

            logger.warning(
                f"Last attempt for {func_name} after {self.api_retry_count} retries"
            )
            return func(*args, **kwargs)

        return wrapper

    def token_osu(self):
                                     
        logger.debug("token_osu() called - checking cache")

                                     
        with self.token_cache_lock:
            if self.token_cache is not None:
                logger.debug("Using cached TOKEN")
                return self.token_cache

        logger.info("TOKEN_CACHE miss - requesting new token")

        self._wait_for_api_slot()
        url = "https://osu.ppy.sh/oauth/token"

        logger.info("POST: %s with client: %s...", url, self.client_id[:3])

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "public",
        }

        try:
            logger.debug("Sending token request to osu! API")
            resp = self.session.post(url, data=data)

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

                                                     
                with self.token_cache_lock:
                    self.token_cache = token

                                        
                self._save_token_to_file()

                return token
            else:
                logger.error("Token not received in API response")
                return None
        except Exception as e:
            logger.error("Error getting token: %s", e)
            return None

    def user_osu(self, identifier, lookup_key):
                                                     
        token = self.token_osu()
        if not token:
            return None

        get_user = self._retry_request(self._get_user)
        return get_user(identifier, lookup_key, token)

    def _get_user(self, identifier, lookup_key, token):
        self._wait_for_api_slot()
        url = f"https://osu.ppy.sh/api/v2/users/{identifier}"
        params = {"key": lookup_key}
        logger.info("GET user: %s with params %s", url, params)
        headers = {"Authorization": f"Bearer {token}"}

        try:
            logger.debug(
                f"Sending request for user '{identifier}' (lookup type: {lookup_key})"
            )
            resp = self.session.get(url, headers=headers, params=params)

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
            logger.error(
                "Unexpected error when requesting user data %s: %s", identifier, e
            )
            raise

    def top_osu(self, user_id, limit=200):
                                                  
        token = self.token_osu()
        if not token:
            return []

        get_top = self._retry_request(self._get_top)
        return get_top(user_id, token, limit)

    def _get_top(self, user_id, token, limit=200):
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

            self._wait_for_api_slot()
            try:
                logger.debug(
                    f"Sending request for top scores (page {offset // page_size + 1})"
                )
                resp = self.session.get(url, headers=headers, params=params)
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

    def map_osu(self, beatmap_id):
                                              
        token = self.token_osu()
        if not token:
            return None

        get_map = self._retry_request(self._get_map)
        return get_map(beatmap_id, token)

    def _get_map(self, beatmap_id, token):
        if not beatmap_id:
            logger.warning("map_osu called with empty beatmap_id")
            return None

        self._wait_for_api_slot()
        url = f"https://osu.ppy.sh/api/v2/beatmaps/{beatmap_id}"
        logger.info("GET map: %s", url)
        headers = {"Authorization": f"Bearer {token}"}

        try:
            logger.debug(f"Sending request for beatmap {beatmap_id}")
            resp = self.session.get(url, headers=headers)

            if resp.status_code == 404:
                logger.warning("Beatmap with ID %s not found", beatmap_id)
                                                                       
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

                                                          
            result = {
                "id": beatmap_id,
                "status": data.get("status", "unknown"),
                "artist": bset.get("artist", ""),
                "title": bset.get("title", ""),
                "version": data.get("version", ""),
                "creator": bset.get("creator", ""),
                "hit_objects": hobj,
                "beatmapset": bset,                                                                      
            }

            logger.debug(
                f"Successfully retrieved beatmap {beatmap_id}: {result['artist']} - {result['title']} [{result['version']}], status: {result['status']}"
            )
            return result

        except requests.exceptions.HTTPError as e:
            logger.error(
                "HTTP error when requesting beatmap data %s: %s", beatmap_id, e
            )
            if "429" in str(e):
                logger.warning("Rate limit hit (429), sleeping for 5 seconds")
                time.sleep(5)
            raise
        except Exception as e:
            logger.error(
                "Unexpected error when requesting beatmap data %s: %s", beatmap_id, e
            )
            raise

    def lookup_osu(self, checksum):
                                      
        try:
            if not checksum:
                logger.error("Empty checksum provided to lookup_osu")
                return None

                                                                                                    
            with self.md5_to_id_cache_lock:
                if checksum in self.md5_to_id_cache:
                    cached_id = self.md5_to_id_cache[checksum]
                    logger.info(
                        f"Using cached beatmap_id {cached_id} for checksum {checksum}"
                    )

                                                                            
                    if cached_id is None:
                        return None

                                                                             
                    try:
                        beatmap_data = self.map_osu(cached_id)
                        if beatmap_data:
                            return beatmap_data
                    except Exception as e:
                        logger.error(
                            f"Error getting beatmap data for cached ID {cached_id}: {e}"
                        )

                                                                                         
                    return cached_id
        except Exception as e:
            logger.error(f"Unexpected error in lookup_osu for checksum {checksum}: {e}")
            return None

                                                               
        wait_event = None
        with self.in_progress_lock:
                                                                                   
            if checksum in self.in_progress_lookups:
                wait_event = self.in_progress_lookups[checksum]["event"]
                self.in_progress_lookups[checksum]["waiters"] += 1
                logger.debug(
                    f"Waiting for in-progress lookup of checksum {checksum}, now has {self.in_progress_lookups[checksum]['waiters']} waiters"
                )
            else:
                                                                    
                wait_event = threading.Event()
                self.in_progress_lookups[checksum] = {
                    "event": wait_event,
                    "waiters": 0,
                    "result": None,
                }
                logger.debug(f"Starting new lookup for checksum {checksum}")

                                           
        if wait_event and self.in_progress_lookups[checksum]["waiters"] > 0:
            logger.debug(f"Waiting for completion of checksum {checksum} lookup")
            wait_event.wait()                                  

                                                                      
            with self.in_progress_lock:
                result = self.in_progress_lookups[checksum]["result"]
                self.in_progress_lookups[checksum]["waiters"] -= 1

                                                           
                if self.in_progress_lookups[checksum]["waiters"] <= 0:
                                                                           
                    if (
                        checksum in self.in_progress_lookups
                        and self.in_progress_lookups[checksum]["waiters"] == 0
                    ):
                        del self.in_progress_lookups[checksum]
                        logger.debug(
                            f"Removed in-progress entry for checksum {checksum}"
                        )

            logger.debug(
                f"Returning cached result for checksum {checksum} after waiting: {result}"
            )
            return result

                                                                                 
        try:
                                                                                   
            with self.md5_to_id_cache_lock:
                if checksum in self.md5_to_id_cache:
                    cached_id = self.md5_to_id_cache[checksum]
                    logger.info(
                        f"Using cached beatmap_id {cached_id} for checksum {checksum} (after recheck)"
                    )

                                                                                 
                    full_data = None
                    if cached_id is not None:
                        try:
                            full_data = self.map_osu(cached_id)
                        except Exception as e:
                            logger.error(
                                f"Error getting beatmap data for cached ID {cached_id}: {e}"
                            )

                                                                   
                    with self.in_progress_lock:
                        if checksum in self.in_progress_lookups:
                            self.in_progress_lookups[checksum]["result"] = (
                                full_data if full_data else cached_id
                            )
                            self.in_progress_lookups[checksum][
                                "event"
                            ].set()                                   

                    return full_data if full_data else cached_id

                                         
            lookup_result = self._retry_request(self._lookup_beatmap)(checksum)
            return lookup_result

        except Exception as e:
            logger.error(
                "Error when looking up beatmap by checksum %s: %s", checksum, e
            )

                                                                   
            with self.in_progress_lock:
                if checksum in self.in_progress_lookups:
                    self.in_progress_lookups[checksum]["result"] = None
                    self.in_progress_lookups[checksum]["event"].set()

                                     
            raise

        finally:
                                                                        
            with self.in_progress_lock:
                if (
                    checksum in self.in_progress_lookups
                    and self.in_progress_lookups[checksum]["waiters"] == 0
                ):
                    del self.in_progress_lookups[checksum]
                    logger.debug(
                        f"Cleanup: removed in-progress entry for checksum {checksum}"
                    )

    def _lookup_beatmap(self, checksum):
                                                         
        try:
            if not checksum:
                logger.error("Empty checksum provided to _lookup_beatmap")
                return self._set_in_progress_result_and_return(
                    checksum, None
                )                            

            self._wait_for_api_slot()
            url = "https://osu.ppy.sh/api/v2/beatmaps/lookup"

            token = self.token_osu()
            if not token:
                logger.error("Failed to get token for lookup_osu")
                return self._set_in_progress_result_and_return(
                    checksum, None
                )                            
        except Exception as e:
            logger.error(
                f"Error initializing _lookup_beatmap for checksum {checksum}: {e}"
            )
            return self._set_in_progress_result_and_return(
                checksum, None
            )                            

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        params = {"checksum": checksum}

        try:
            response = self.session.get(url, headers=headers, params=params)

            if response.status_code == 404:
                logger.warning("Beatmap with checksum %s not found.", checksum)
                                                  
                with self.md5_to_id_cache_lock:
                    self.md5_to_id_cache[checksum] = None

                                                  
                self._save_md5_cache_to_file()

                result = None

                                                               
                with self.in_progress_lock:
                    if checksum in self.in_progress_lookups:
                        self.in_progress_lookups[checksum]["result"] = result
                        self.in_progress_lookups[checksum]["event"].set()

                return result

                                                        
            if response.status_code == 401:
                logger.warning(
                    "Authorization failed (401) for lookup_osu. Invalidating token."
                )
                with self.token_cache_lock:
                    self.token_cache = None
                return self._set_in_progress_result_and_return(
                    checksum, None
                )                            

            response.raise_for_status()

            data = response.json()

            if not data:
                logger.warning("Empty API response for checksum %s", checksum)
                result = None

                                                               
                with self.in_progress_lock:
                    if checksum in self.in_progress_lookups:
                        self.in_progress_lookups[checksum]["result"] = result
                        self.in_progress_lookups[checksum]["event"].set()

                return result

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Request error in _lookup_beatmap for checksum {checksum}: {e}"
            )
            return self._set_in_progress_result_and_return(
                checksum, None
            )                            

        beatmap_id = data.get("id")

                                   
        if beatmap_id:
            with self.md5_to_id_cache_lock:
                self.md5_to_id_cache[checksum] = beatmap_id
                logger.info(f"Cached beatmap_id {beatmap_id} for checksum {checksum}")

                                              
            self._save_md5_cache_to_file()

                                                       
        bset = data.get("beatmapset", {})

                                                        
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

                                                                       
        with self.in_progress_lock:
            if checksum in self.in_progress_lookups:
                self.in_progress_lookups[checksum]["result"] = result
                self.in_progress_lookups[checksum]["event"].set()

        return result

    def _set_in_progress_result_and_return(self, checksum, result_value):
                                                                               
                                                                    
                                                           
                                                          
        self._set_in_progress_result(checksum, result_value)
        return result_value

    def reset_caches(self):
                                                       
        with self.token_cache_lock:
            self.token_cache = None

        with self.md5_to_id_cache_lock:
            cache_size = len(self.md5_to_id_cache)
            self.md5_to_id_cache.clear()

        logger.info(
            f"All osu_api caches have been reset (cleared {cache_size} MD5-to-ID mappings)"
        )

                             
        try:
            if self.token_cache_path and os.path.exists(self.token_cache_path):
                os.remove(self.token_cache_path)
                logger.info(f"Token cache file removed: {self.token_cache_path}")

            if self.md5_cache_path and os.path.exists(self.md5_cache_path):
                os.remove(self.md5_cache_path)
                logger.info(f"MD5 cache file removed: {self.md5_cache_path}")
        except (FileNotFoundError, PermissionError) as e:
            logger.warning(f"Failed to remove cache files: {e}")

    def download_image(self, url, path, timeout=30):
                                                                                   
        try:
            if os.path.exists(path):
                logger.debug(
                    "Image already exists locally: %s", mask_path_for_log(path)
                )
                return True

            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            logger.info("GET image: %s", url)

                                            
            self._wait_for_api_slot()

                                                        
            @self._retry_request
            def download_image_content():
                resp = self.session.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp.content

            content = download_image_content()
            file_size = len(content)
            logger.debug("Download successful: received %d bytes", file_size)

            with open(path, "wb") as f:
                f.write(content)

            logger.debug("Image saved to %s", mask_path_for_log(path))
            return True
        except Exception as e:
            logger.error("Failed to download image: %s: %s", url, e)
            return False

    def _set_in_progress_result(self, checksum, result):
                                                                            
        try:
            with self.in_progress_lock:
                if checksum in self.in_progress_lookups:
                    self.in_progress_lookups[checksum]["result"] = result
                    self.in_progress_lookups[checksum]["event"].set()
        except Exception as e:
            logger.error(
                f"Error setting in-progress result for checksum {checksum}: {e}"
            )

        return result

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def delete_keys_from_keyring():
        try:
            keyring.delete_password(KEYRING_SERVICE, CLIENT_ID_KEY)
            keyring.delete_password(KEYRING_SERVICE, CLIENT_SECRET_KEY)
            logger.info("API keys deleted from system keyring")
            return True
        except Exception as e:
            logger.error("Error deleting API keys from keyring: %s", e)
            return False

    def _set_in_progress_result(self, checksum, result):
                                                                            
        try:
            with self.in_progress_lock:
                if checksum in self.in_progress_lookups:
                    self.in_progress_lookups[checksum]["result"] = result
                    self.in_progress_lookups[checksum]["event"].set()
        except Exception as e:
            logger.error(
                f"Error setting in-progress result for checksum {checksum}: {e}"
            )

        return result
