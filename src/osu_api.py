import functools
import logging
import os
import threading
import time

import keyring
import requests
from keyring.backends.Windows import WinVaultKeyring
from keyring.errors import PasswordDeleteError
from requests.adapters import HTTPAdapter

from app_config import (
    PUBLIC_REQUESTS_PER_MINUTE,
    MAP_DOWNLOAD_TIMEOUT,
    API_PROXY_BASE,
    API_RATE_LIMIT,
)
from database import db_get_map, db_upsert_from_scan
from path_utils import get_env_path, mask_path_for_log
from utils import RateLimiter
from auth_manager import AuthMode

keyring.set_keyring(WinVaultKeyring())
api_logger = logging.getLogger("api_logger")

KEYRING_SERVICE = "osu_lost_scores_analyzer"
CLIENT_ID_KEY = "client_id"
CLIENT_SECRET_KEY = "client_secret"


class OAuthSessionExpiredException(Exception):
    pass


ACCESS_TOKEN_KEY = "access_token"

ENV_PATH = os.environ.get("DOTENV_PATH")
if not ENV_PATH or not os.path.exists(ENV_PATH):
    ENV_PATH = get_env_path()


class OsuApiClient:
    _instance = None

    def __init__(
        self,
        client_id=None,
        client_secret=None,
        api_rate_limit=1.0,
        api_retry_count=3,
        api_retry_delay=0.5,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_rate_limit = api_rate_limit
        self.api_retry_count = api_retry_count
        self.api_retry_delay = api_retry_delay
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        # noinspection HttpUrlsUsage
        self.session.mount("http://", adapter)
        self.api_lock = threading.Lock()
        self.last_call = 0
        self.token_cache = None
        self.token_cache_lock = threading.Lock()
        self._logged_cached_token_usage = False
        self.in_progress_lookups = {}
        self.in_progress_lock = threading.Lock()
        self.public_rate_limiter = RateLimiter(PUBLIC_REQUESTS_PER_MINUTE)

        self.auth_mode = AuthMode.LOGGED_OUT
        self.state_lock = threading.Lock()
        self.base_url = "https://osu.ppy.sh/api/v2"

        if client_id and client_secret:
            self.configure_for_custom_keys(client_id, client_secret)
        else:
            api_logger.info("OsuApiClient initialized in LOGGED_OUT state")

    @classmethod
    def get_instance(
        cls,
        client_id=None,
        client_secret=None,
        api_rate_limit=1.0,
        api_retry_count=3,
        api_retry_delay=0.5,
    ):
        if cls._instance is not None and hasattr(cls._instance, "auth_mode"):
            if cls._instance.auth_mode == AuthMode.OAUTH:
                return cls._instance

        if cls._instance is None:
            if not client_id or not client_secret:
                client_id, client_secret = cls.get_keys_from_keyring()
            if client_id and client_secret:
                cls._instance = cls(
                    client_id=client_id,
                    client_secret=client_secret,
                    api_rate_limit=api_rate_limit,
                    api_retry_count=api_retry_count,
                    api_retry_delay=api_retry_delay,
                )
            else:
                cls._instance = cls(
                    api_rate_limit=api_rate_limit,
                    api_retry_count=api_retry_count,
                    api_retry_delay=api_retry_delay,
                )
        elif client_id and client_secret:
            cls._instance.client_id = client_id
            cls._instance.client_secret = client_secret
            with cls._instance.token_cache_lock:
                cls._instance.token_cache = None

            try:
                keyring.delete_password(KEYRING_SERVICE, ACCESS_TOKEN_KEY)
                api_logger.info(
                    "Deleted stale access token from keyring due to new keys being provided"
                )
            except PasswordDeleteError:
                pass

        return cls._instance

    @classmethod
    def reset_instance(cls):
        if cls._instance:
            cls._instance._logged_cached_token_usage = False
        cls._instance = None

    def configure_for_oauth(self, jwt_token: str):
        with self.state_lock:
            self.auth_mode = AuthMode.OAUTH
            self.base_url = API_PROXY_BASE
            self.api_rate_limit = 0.0
            self.session.headers.clear()
            self.session.headers.update({"Authorization": f"Bearer {jwt_token}"})
            OsuApiClient._instance = self
            api_logger.info(
                f"OsuApiClient configured for OAuth mode with backend: {self.base_url}"
            )

    def configure_for_custom_keys(self, client_id: str, client_secret: str):
        with self.state_lock:
            self.auth_mode = AuthMode.CUSTOM_KEYS
            self.base_url = "https://osu.ppy.sh/api/v2"
            self.session.headers.clear()
            self.client_id = client_id
            self.client_secret = client_secret
            self.api_rate_limit = API_RATE_LIMIT
            api_logger.info("OsuApiClient configured for Custom Keys mode")

        self._load_token_from_keyring()

    def deconfigure(self):
        with self.state_lock:
            self.auth_mode = AuthMode.LOGGED_OUT
            self.base_url = "https://osu.ppy.sh/api/v2"
            self.session.headers.clear()
            with self.token_cache_lock:
                self.token_cache = None
            api_logger.info("OsuApiClient deconfigured, state set to LOGGED_OUT")

    def _handle_oauth_401_error(self):
        api_logger.warning(
            "OAuth session expired, clearing session and switching to LOGGED_OUT mode"
        )

        try:
            from auth_manager import AuthManager

            auth_manager = AuthManager()
            auth_manager.clear_oauth_session_only()
            api_logger.info("OAuth session cleared from keyring")
        except Exception as e:
            api_logger.error(f"Failed to clear OAuth session: {e}")

        with self.state_lock:
            self.auth_mode = AuthMode.LOGGED_OUT
            self.session.headers.clear()
            api_logger.info(
                "API client switched to LOGGED_OUT mode due to OAuth session expiry"
            )

    def _request(self, method, endpoint, params=None, json_data=None):
        with self.state_lock:
            if self.auth_mode == AuthMode.LOGGED_OUT:
                raise Exception("API client is not configured")
            current_auth_mode = self.auth_mode
            current_base_url = self.base_url

        url = f"{current_base_url}{endpoint}"

        for attempt in range(self.api_retry_count + 1):
            try:
                if current_auth_mode == AuthMode.CUSTOM_KEYS:
                    token = self.token_osu()
                    if not token:
                        raise Exception("Could not get osu! API token")
                    headers = {"Authorization": f"Bearer {token}"}
                elif current_auth_mode == AuthMode.OAUTH:
                    headers = dict(self.session.headers)
                else:
                    raise Exception(f"Unknown auth mode: {current_auth_mode}")

                self._wait_for_api_slot()

                api_logger.debug(
                    f"API Client: Sending {method.upper()} request to {url}"
                )
                response = self.session.request(
                    method,
                    url,
                    params=params,
                    json=json_data,
                    headers=headers,
                    timeout=30,
                )
                api_logger.debug(
                    f"API Client: Received response with status {response.status_code}"
                )

                if response.status_code == 404:
                    return None

                response.raise_for_status()

                if response.status_code != 204:
                    json_data = response.json()
                    if (
                        isinstance(json_data, dict)
                        and json_data.get("authentication") == "basic"
                    ):
                        if current_auth_mode == AuthMode.OAUTH:
                            self._handle_oauth_401_error()
                            raise OAuthSessionExpiredException(
                                "OAuth session has expired. Please re-authenticate."
                            )
                    return json_data
                else:
                    return None

            except requests.HTTPError as e:
                status = e.response.status_code
                api_logger.warning(
                    f"HTTP Error {status} on {url} (Attempt {attempt + 1})"
                )
                if status == 401:
                    if current_auth_mode == AuthMode.OAUTH:
                        self._handle_oauth_401_error()
                        raise OAuthSessionExpiredException(
                            "OAuth session has expired. Please re-authenticate."
                        )
                    elif (
                        current_auth_mode == AuthMode.CUSTOM_KEYS
                        and attempt < self.api_retry_count
                    ):
                        with self.token_cache_lock:
                            self.token_cache = None
                        continue
                if attempt >= self.api_retry_count or status in [404, 403]:
                    raise
            except requests.RequestException as e:
                api_logger.warning(f"Request failed: {e} (Attempt {attempt + 1})")
                if attempt >= self.api_retry_count:
                    raise
            time.sleep(self.api_retry_delay * (2**attempt))

        raise Exception(f"Request to {url} failed after all retries")

    def get_user_data(self, identifier, lookup_key="id"):
        endpoint = f"/users/{identifier}"
        params = {"key": lookup_key}
        return self._request("get", endpoint, params=params)

    def get_current_user_data(self):
        endpoint = "/me"
        return self._request("get", endpoint)

    def get_user_scores(self, user_id, limit=100):
        all_scores = []
        page_size = 50
        for offset in range(0, limit, page_size):
            endpoint = f"/users/{user_id}/scores/best"
            params = {
                "limit": min(page_size, limit - offset),
                "offset": offset,
                "mode": "osu",
                "include": "beatmap",
            }
            page_scores = self._request("get", endpoint, params=params)
            if not page_scores:
                break
            all_scores.extend(page_scores)
        return all_scores

    def get_beatmap_data(self, beatmap_id):
        if not beatmap_id:
            api_logger.warning("get_beatmap_data called with empty beatmap_id")
            return None

        endpoint = f"/beatmaps/{beatmap_id}"

        try:
            data = self._request("get", endpoint)
        except Exception as e:
            api_logger.error(f"Failed to get beatmap data for ID {beatmap_id}: {e}")
            return None

        if not data:
            api_logger.warning("Empty API response for beatmap %s", beatmap_id)
            return None

        bset = data.get("beatmapset", {})
        c = data.get("count_circles", 0)
        s = data.get("count_sliders", 0)
        sp = data.get("count_spinners", 0)
        hobj = c + s + sp

        return {
            "id": beatmap_id,
            "status": data.get("status", "unknown"),
            "artist": bset.get("artist", ""),
            "title": bset.get("title", ""),
            "version": data.get("version", ""),
            "creator": bset.get("creator", ""),
            "hit_objects": hobj,
            "beatmapset": bset,
        }

    def lookup_beatmap(self, checksum):
        if not checksum:
            return None

        endpoint = "/beatmaps/lookup"
        params = {"checksum": checksum}

        try:
            data = self._request("get", endpoint, params=params)
            beatmap_id = data.get("id") if data else None
            return self.get_beatmap_data(beatmap_id) if beatmap_id else None
        except Exception as e:
            api_logger.error(f"Error during beatmap lookup for {checksum}: {e}")
            return None

    def _load_token_from_keyring(self):
        try:
            token = keyring.get_password(KEYRING_SERVICE, ACCESS_TOKEN_KEY)
            if token:
                with self.token_cache_lock:
                    self.token_cache = token
                api_logger.debug("Access token loaded from keyring")
        except Exception as e:
            api_logger.warning(f"Failed to load token from keyring: {e}")

    def _save_token_to_keyring(self):
        if not self.token_cache:
            return
        try:
            keyring.set_password(KEYRING_SERVICE, ACCESS_TOKEN_KEY, self.token_cache)
            api_logger.debug("Access token saved to keyring")
        except Exception as e:
            api_logger.warning(f"Failed to save token to keyring: {e}")

    def _wait_for_api_slot(self):
        with self.api_lock:
            now = time.time()
            diff = now - self.last_call
            if self.api_rate_limit > 0 and diff < self.api_rate_limit:
                delay = self.api_rate_limit - diff
                api_logger.debug(
                    f"Rate limiting: waiting {delay:.2f}s before next API call"
                )
                time.sleep(delay)
            self.last_call = time.time()

    def _retry_request(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            api_logger.debug(
                f"API call to {func_name} with retry mechanism (max_retries={self.api_retry_count})"
            )
            retries = 0
            while retries < self.api_retry_count:
                try:
                    if retries > 0:
                        api_logger.debug(
                            f"Executing {func_name} (attempt {retries + 1}/{self.api_retry_count + 1})"
                        )
                    response = func(*args, **kwargs)
                    return response
                except requests.exceptions.HTTPError as e:
                    status_code = (
                        e.response.status_code if hasattr(e, "response") else None
                    )
                    if status_code == 401:
                        api_logger.error(
                            f"Authentication error (401) in {func_name}: {e}"
                        )
                        with self.token_cache_lock:
                            self.token_cache = None
                        api_logger.info("Token invalidated due to 401 error")
                        raise
                    elif status_code == 404:
                        api_logger.warning(
                            f"Resource not found (404) in {func_name}: {e}"
                        )
                        raise
                    elif status_code == 429:
                        wait_time = self.api_retry_delay * (4**retries)
                        api_logger.warning(
                            f"Rate limit exceeded (429) in {func_name}. Waiting {wait_time}s before retry"
                        )
                        time.sleep(wait_time)
                        retries += 1
                        continue
                    wait_time = self.api_retry_delay * (2**retries)
                    api_logger.warning(
                        f"HTTP error in {func_name} (status={status_code}): {e}. Retry {retries + 1}/{self.api_retry_count} after {wait_time}s"
                    )
                    time.sleep(wait_time)
                    retries += 1
                except requests.exceptions.ConnectionError as e:
                    wait_time = self.api_retry_delay * (3**retries)
                    api_logger.warning(
                        f"Connection error in {func_name}: {e}. Retry {retries + 1}/{self.api_retry_count} after {wait_time}s"
                    )
                    time.sleep(wait_time)
                    retries += 1
                except requests.exceptions.RequestException as e:
                    wait_time = self.api_retry_delay * (2**retries)
                    api_logger.warning(
                        f"Request error in {func_name}: {e}. Retry {retries + 1}/{self.api_retry_count} after {wait_time}s"
                    )
                    time.sleep(wait_time)
                    retries += 1
                except Exception as e:
                    api_logger.error(f"Unexpected error in {func_name}: {e}")
                    raise
            api_logger.warning(
                f"Last attempt for {func_name} after {self.api_retry_count} retries"
            )
            return func(*args, **kwargs)

        return wrapper

    def token_osu(self):
        api_logger.debug("token_osu() called - checking cache")
        with self.token_cache_lock:
            if self.token_cache is not None:
                if not self._logged_cached_token_usage:
                    api_logger.debug("Using cached TOKEN")
                    self._logged_cached_token_usage = True
                return self.token_cache
        api_logger.info("TOKEN_CACHE miss - requesting new token")
        self._wait_for_api_slot()
        url = "https://osu.ppy.sh/oauth/token"
        if self.client_id:
            api_logger.info("POST: %s with client: %s...", url, self.client_id[:3])
        else:
            api_logger.info("POST: %s (OAuth mode)", url)
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "public",
        }
        try:
            api_logger.debug("Sending token request to osu! API")
            resp = self.session.post(url, data=data, timeout=30)
            if resp.status_code == 401:
                api_logger.error(
                    "Invalid API credentials. Check your Client ID and Client Secret"
                )
                api_logger.error("Server response: %s", resp.text)
                return None
            resp.raise_for_status()
            token = resp.json().get("access_token")
            if token:
                api_logger.info("API token successfully received")
                with self.token_cache_lock:
                    self.token_cache = token
                self._save_token_to_keyring()
                return token
            else:
                api_logger.error("Token not received in API response")
                return None
        except Exception as e:
            api_logger.error("Error getting token: %s", e)
            return None

    def user_osu(self, identifier, lookup_key):
        try:
            return self.get_user_data(identifier, lookup_key)
        except OAuthSessionExpiredException:
            raise
        except Exception as e:
            api_logger.error(f"Error in user_osu: {e}")
            return None

    def _get_user(self, identifier, lookup_key, token):
        self._wait_for_api_slot()
        url = f"https://osu.ppy.sh/api/v2/users/{identifier}"
        params = {"key": lookup_key}
        api_logger.info("GET user: %s with params %s", url, params)
        headers = {"Authorization": f"Bearer {token}"}
        try:
            api_logger.debug(
                f"Sending request for user '{identifier}' (lookup type: {lookup_key})"
            )
            resp = self.session.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 404:
                api_logger.error(
                    "User '%s' (lookup type: %s) not found", identifier, lookup_key
                )
                return None
            resp.raise_for_status()
            response_data = resp.json()
            api_logger.debug(
                f"Successfully retrieved user data for '{identifier}' (username: {response_data.get('username', 'unknown')})"
            )
            return response_data
        except requests.exceptions.HTTPError as e:
            api_logger.error(
                "HTTP error when requesting user data %s: %s", identifier, e
            )
            raise
        except Exception as e:
            api_logger.error(
                "Unexpected error when requesting user data %s: %s", identifier, e
            )
            raise

    def top_osu(self, user_id, limit=200):
        if self.auth_mode == AuthMode.OAUTH:
            return self.get_user_scores(user_id, limit=limit)

        token = self.token_osu()
        if not token:
            return []
        get_top = self._retry_request(self._get_top)
        return get_top(user_id, token, limit)

    def _get_top(self, user_id, token, limit=200):
        all_scores = []
        page_size = 100
        api_logger.info(f"Retrieving top scores for user {user_id} (limit={limit})")
        for offset in range(0, limit, page_size):
            url = f"https://osu.ppy.sh/api/v2/users/{user_id}/scores/best"
            current_limit = min(page_size, limit - offset)
            api_logger.info(
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
                api_logger.debug(
                    f"Sending request for top scores (page {offset // page_size + 1})"
                )
                resp = self.session.get(url, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                page_scores = resp.json()
                if not page_scores:
                    api_logger.info("No more scores found after offset %d", offset)
                    break
                all_scores.extend(page_scores)
                api_logger.debug(
                    "Retrieved %d scores (offset %d, total so far: %d)",
                    len(page_scores),
                    offset,
                    len(all_scores),
                )
                if len(page_scores) < current_limit:
                    api_logger.debug("Last page reached at offset %d", offset)
                    break
            except requests.exceptions.HTTPError as e:
                api_logger.error(
                    "HTTP error when requesting top scores for user %s: %s", user_id, e
                )
                raise
            except Exception as e:
                api_logger.error(
                    "Unexpected error when requesting top scores for user %s: %s",
                    user_id,
                    e,
                )
                raise
        api_logger.info(
            f"Total of {len(all_scores)} scores retrieved for user {user_id}"
        )
        return all_scores

    def maps_osu(self, beatmap_ids, gui_log=None, logger=None, progress_callback=None):
        if self.auth_mode == AuthMode.OAUTH:
            token = None
        else:
            token = self.token_osu()
            if not token:
                return {}

        unique_ids = sorted(list(set(beatmap_ids)))
        if not unique_ids:
            return {}

        all_beatmaps_data = {}
        batch_size = 50

        get_maps_batch_with_retry = self._retry_request(self._get_maps_batch)

        for i in range(0, len(unique_ids), batch_size):
            batch_ids = unique_ids[i : i + batch_size]
            api_logger.info(
                f"Requesting batch of {len(batch_ids)} beatmaps from API (total processed: {i})"
            )

            current_progress = min(i + batch_size, len(unique_ids))
            if progress_callback:
                progress_callback(current_progress, len(unique_ids))

            progress_message = (
                f"Validating map statuses {current_progress}/{len(unique_ids)}..."
            )
            if gui_log:
                gui_log(progress_message, update_last=True)
            if logger:
                logger.info(progress_message)

            try:
                batch_result = get_maps_batch_with_retry(batch_ids, token)
                if batch_result:
                    for beatmap_data in batch_result:
                        all_beatmaps_data[beatmap_data["id"]] = beatmap_data
            except Exception as e:
                api_logger.error(
                    f"Failed to process a batch of beatmaps starting with ID {batch_ids[0]}: {e}"
                )

        api_logger.info(
            f"Successfully retrieved data for {len(all_beatmaps_data)} unique beatmaps"
        )
        return all_beatmaps_data

    def _get_maps_batch(self, beatmap_ids, token=None):
        if not beatmap_ids:
            return []

        if self.auth_mode == AuthMode.OAUTH:
            endpoint = "/beatmaps"
            params = [("ids[]", bid) for bid in beatmap_ids]
            params_dict = {}

            for key, value in params:
                if key in params_dict:
                    if not isinstance(params_dict[key], list):
                        params_dict[key] = [params_dict[key]]
                    params_dict[key].append(value)
                else:
                    params_dict[key] = value

            try:
                response = self._request("get", endpoint, params=params_dict)

                if response and "beatmaps" in response:
                    beatmaps = response["beatmaps"]
                    return beatmaps

                return []
            except Exception as e:
                api_logger.error(f"OAuth batch request failed: {e}")
                return []

        self._wait_for_api_slot()
        url = "https://osu.ppy.sh/api/v2/beatmaps"

        params = [("ids[]", bid) for bid in beatmap_ids]
        headers = {"Authorization": f"Bearer {token}"}

        try:
            resp = self.session.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            beatmaps = data.get("beatmaps", [])
            return beatmaps
        except requests.exceptions.HTTPError as e:
            api_logger.error(f"HTTP error when requesting beatmap batch: {e}")
            raise
        except Exception as e:
            api_logger.error(f"Unexpected error when requesting beatmap batch: {e}")
            raise

    def _get_map(self, beatmap_id, token):
        if not beatmap_id:
            api_logger.warning("map_osu called with empty beatmap_id")
            return None
        self._wait_for_api_slot()
        url = f"https://osu.ppy.sh/api/v2/beatmaps/{beatmap_id}"
        api_logger.info("GET map: %s", url)
        headers = {"Authorization": f"Bearer {token}"}
        try:
            api_logger.debug(f"Sending request for beatmap {beatmap_id}")
            resp = self.session.get(url, headers=headers, timeout=30)
            if resp.status_code == 404:
                api_logger.warning("Beatmap with ID %s not found", beatmap_id)
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
                api_logger.warning("Empty API response for beatmap %s", beatmap_id)
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
            api_logger.debug(
                f"Successfully retrieved beatmap {beatmap_id}: {result['artist']} - {result['title']} [{result['version']}], status: {result['status']}"
            )
            return result
        except requests.exceptions.HTTPError as e:
            api_logger.error(
                "HTTP error when requesting beatmap data %s: %s", beatmap_id, e
            )
            if "429" in str(e):
                api_logger.warning("Rate limit hit (429), sleeping for 5 seconds")
                time.sleep(5)
            raise
        except Exception as e:
            api_logger.error(
                "Unexpected error when requesting beatmap data %s: %s", beatmap_id, e
            )
            raise

    def lookup_osu(self, checksum):
        if not checksum:
            api_logger.error("Empty checksum provided to lookup_osu")
            return None

        map_data = db_get_map(checksum, by="md5")
        if map_data and map_data.get("lookup_status") in ["found", "not_found"]:
            api_logger.debug(
                f"DB cache hit for checksum {checksum}: status is '{map_data['lookup_status']}'"
            )
            return map_data if map_data.get("lookup_status") == "found" else None

        wait_needed = False
        with self.in_progress_lock:
            if checksum in self.in_progress_lookups:
                wait_needed = True
                lookup_event = self.in_progress_lookups[checksum]["event"]
                self.in_progress_lookups[checksum]["waiters"] += 1
            else:
                lookup_event = threading.Event()
                self.in_progress_lookups[checksum] = {
                    "event": lookup_event,
                    "waiters": 0,
                    "result": None,
                }

        if wait_needed:
            lookup_event.wait(timeout=15)
            with self.in_progress_lock:
                if checksum in self.in_progress_lookups:
                    result = self.in_progress_lookups[checksum]["result"]
                    self.in_progress_lookups[checksum]["waiters"] -= 1
                    if self.in_progress_lookups[checksum]["waiters"] <= 0:
                        del self.in_progress_lookups[checksum]
                    return result
                else:
                    return None

        try:
            lookup_result = self._retry_request(self._lookup_beatmap)(checksum)
            return lookup_result
        except Exception as e:
            api_logger.error(f"Error in lookup for checksum {checksum}: {e}")
            with self.in_progress_lock:
                if checksum in self.in_progress_lookups:
                    self.in_progress_lookups[checksum]["result"] = None
                    self.in_progress_lookups[checksum]["event"].set()
            return None
        finally:
            with self.in_progress_lock:
                if (
                    checksum in self.in_progress_lookups
                    and self.in_progress_lookups[checksum]["waiters"] == 0
                ):
                    del self.in_progress_lookups[checksum]

    def _lookup_beatmap(self, checksum):
        try:
            if self.auth_mode == AuthMode.OAUTH:
                endpoint = "/beatmaps/lookup"
                params = {"checksum": checksum}

                try:
                    response_data = self._request("get", endpoint, params=params)

                    if not response_data:
                        api_logger.warning(
                            "Beatmap with checksum %s not found via OAuth", checksum
                        )
                        db_upsert_from_scan(checksum, {"lookup_status": "not_found"})
                        return self._set_in_progress_result_and_return(checksum, None)

                    api_data = response_data
                except Exception as e:
                    if "404" in str(e) or "not found" in str(e).lower():
                        api_logger.warning(
                            "Beatmap with checksum %s not found via OAuth (404)",
                            checksum,
                        )
                        db_upsert_from_scan(checksum, {"lookup_status": "not_found"})
                        return self._set_in_progress_result_and_return(checksum, None)
                    raise
            else:
                self._wait_for_api_slot()
                url = "https://osu.ppy.sh/api/v2/beatmaps/lookup"
                token = self.token_osu()
                if not token:
                    api_logger.error("Failed to get token for lookup_osu")
                    return self._set_in_progress_result_and_return(checksum, None)

                headers = {"Authorization": f"Bearer {token}"}
                params = {"checksum": checksum}

                response = self.session.get(url, headers=headers, params=params)

                if response.status_code == 404:
                    api_logger.warning(
                        "Beatmap with checksum %s not found (404)", checksum
                    )
                    db_upsert_from_scan(checksum, {"lookup_status": "not_found"})
                    return self._set_in_progress_result_and_return(checksum, None)

                response.raise_for_status()
                api_data = response.json()

            if not api_data:
                api_logger.warning("Empty API response for checksum %s", checksum)
                return self._set_in_progress_result_and_return(checksum, None)

            bset = api_data.get("beatmapset", {})
            hobj = (
                api_data.get("count_circles", 0)
                + api_data.get("count_sliders", 0)
                + api_data.get("count_spinners", 0)
            )

            result_data = {
                "beatmap_id": api_data.get("id"),
                "beatmapset_id": bset.get("id"),
                "artist": bset.get("artist", ""),
                "title": bset.get("title", ""),
                "version": api_data.get("version", ""),
                "creator": bset.get("creator", ""),
                "hit_objects": hobj,
                "api_status": api_data.get("status", "unknown"),
                "lookup_status": "found",
            }
            db_upsert_from_scan(checksum, result_data)

            api_logger.info(f"Cached full beatmap data for checksum {checksum}")

            return self._set_in_progress_result_and_return(checksum, result_data)

        except requests.exceptions.RequestException as e:
            api_logger.error(
                f"Request error in _lookup_beatmap for checksum {checksum}: {e}"
            )
            return self._set_in_progress_result_and_return(checksum, None)

    def _set_in_progress_result_and_return(self, checksum, result_value):
        self._set_in_progress_result(checksum, result_value)
        return result_value

    def download_osu_file(self, beatmap_id, target_path):
        try:
            if not beatmap_id:
                api_logger.error("Cannot download .osu file: beatmap_id is None or 0")
                return None

            if os.path.exists(target_path):
                api_logger.debug(
                    "Beatmap file already exists: %s", mask_path_for_log(target_path)
                )
                return target_path

            url = f"https://osu.ppy.sh/osu/{beatmap_id}"
            api_logger.info("GET beatmap file: %s", url)

            @self._retry_request
            def download_beatmap_content():
                self.public_rate_limiter.wait()
                resp = self.session.get(url, timeout=MAP_DOWNLOAD_TIMEOUT)
                if resp.status_code == 404:
                    api_logger.warning(
                        f"Beatmap with ID {beatmap_id} not found on server (HTTP 404)"
                    )
                    return None
                resp.raise_for_status()
                return resp.content

            api_logger.debug(f"Downloading .osu file for beatmap_id {beatmap_id}")
            content = download_beatmap_content()
            if content is None:
                return None

            file_size = len(content)
            api_logger.debug(f"Download successful: received {file_size} bytes")

            with open(target_path, "wb") as f:
                f.write(content)

            api_logger.debug(f"File saved to {mask_path_for_log(target_path)}")
            api_logger.info(
                f"Successfully downloaded and cached .osu file for beatmap_id {beatmap_id}"
            )

            return target_path

        except Exception as e:
            api_logger.error(
                f"Unexpected error downloading .osu file for beatmap_id {beatmap_id}: {e}"
            )
            return None

    def reset_caches(self):
        with self.token_cache_lock:
            self.token_cache = None
            self._logged_cached_token_usage = False
        api_logger.info("All osu_api caches have been reset")

    def download_image(self, url, path):
        try:
            if os.path.exists(path):
                api_logger.debug(
                    "Image already exists locally: %s", mask_path_for_log(path)
                )
                return path

            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            api_logger.info("GET image: %s", url)
            self.public_rate_limiter.wait()

            @self._retry_request
            def download_image_content():
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                return resp.content

            content = download_image_content()
            if content:
                with open(path, "wb") as f:
                    f.write(content)
                api_logger.debug("Image saved to %s", mask_path_for_log(path))
                return path
            return None
        except (requests.exceptions.RequestException, IOError, OSError):
            api_logger.exception("Failed to download image: %s", url)
            return None

    def _set_in_progress_result(self, checksum, result):
        # noinspection PyBroadException
        try:
            with self.in_progress_lock:
                if checksum in self.in_progress_lookups:
                    self.in_progress_lookups[checksum]["result"] = result
                    self.in_progress_lookups[checksum]["event"].set()
        except Exception:
            api_logger.exception(
                "Error setting in-progress result for checksum %s", checksum
            )
        return result

    @staticmethod
    def save_keys_to_keyring(client_id, client_secret):
        try:
            if client_id and client_secret:
                keyring.set_password(KEYRING_SERVICE, CLIENT_ID_KEY, client_id)
                keyring.set_password(KEYRING_SERVICE, CLIENT_SECRET_KEY, client_secret)
                api_logger.info(
                    "API keys saved to system keyring (CLIENT_ID: %s...)", client_id[:3]
                )
                return True
            else:
                api_logger.warning("Cannot save empty API keys")
                return False
        except Exception as e:
            api_logger.error("Error saving API keys to keyring: %s", e)
            return False

    @staticmethod
    def get_keys_from_keyring():
        try:
            client_id = keyring.get_password(KEYRING_SERVICE, CLIENT_ID_KEY)
            client_secret = keyring.get_password(KEYRING_SERVICE, CLIENT_SECRET_KEY)
            if client_id and client_secret:
                api_logger.info(
                    "API keys retrieved from system keyring (CLIENT_ID: %s...)",
                    client_id[:3],
                )
            else:
                api_logger.warning("API keys not found in system keyring")
            return client_id, client_secret
        except Exception as e:
            api_logger.error("Error retrieving API keys from keyring: %s", e)
            return None, None

    @staticmethod
    def delete_keys_from_keyring():
        try:
            keyring.delete_password(KEYRING_SERVICE, CLIENT_ID_KEY)
            keyring.delete_password(KEYRING_SERVICE, CLIENT_SECRET_KEY)
            keyring.delete_password(KEYRING_SERVICE, ACCESS_TOKEN_KEY)
            api_logger.info("API keys deleted from system keyring")
            return True
        except Exception as e:
            api_logger.error("Error deleting API keys from keyring: %s", e)
            return False
