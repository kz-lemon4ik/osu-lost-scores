import enum
import json
import logging
from dataclasses import dataclass

import keyring

from app_config import BACKEND_BASE_URL

KEYRING_SERVICE = "osu_lost_scores_analyzer"
CLIENT_ID_KEY = "client_id"
CLIENT_SECRET_KEY = "client_secret"
OAUTH_SESSION_KEY = "oauth_session"

logger = logging.getLogger(__name__)


class AuthMode(enum.Enum):
    OAUTH = "oauth"
    CUSTOM_KEYS = "custom_keys"
    LOGGED_OUT = "logged_out"


@dataclass
class Session:
    auth_mode: AuthMode = AuthMode.LOGGED_OUT
    user_id: int | None = None
    username: str | None = None
    jwt_token: str | None = None

    def to_dict(self):
        return {
            "auth_mode": self.auth_mode.value,
            "user_id": self.user_id,
            "username": self.username,
            "jwt_token": self.jwt_token,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            auth_mode=AuthMode(data.get("auth_mode", "logged_out")),
            user_id=data.get("user_id"),
            username=data.get("username"),
            jwt_token=data.get("jwt_token"),
        )


class AuthManager:
    def __init__(self, config_dir: str):
        self._config_dir = config_dir
        self.backend_base_url = BACKEND_BASE_URL
        self._cached_session = None
        self._session_cache_valid = False
        logger.debug(f"AuthManager initialized. Config dir: {self._config_dir}")
        logger.debug(f"Backend base URL: {self.backend_base_url}")

    def get_oauth_login_url(self, callback_port: int) -> str:
        return f"{self.backend_base_url}/api/auth/login?callback_port={callback_port}&client_type=desktop"

    def save_oauth_session(self, jwt_token: str, user_id: int, username: str):
        session = Session(
            auth_mode=AuthMode.OAUTH,
            user_id=user_id,
            username=username,
            jwt_token=jwt_token,
        )
        try:
            session_json = json.dumps(session.to_dict())
            keyring.set_password(KEYRING_SERVICE, OAUTH_SESSION_KEY, session_json)
            self._cached_session = session
            self._session_cache_valid = True
            logger.info(f"OAuth session saved for user '{username}' (ID: {user_id})")
        except Exception as e:
            logger.error(f"Failed to save OAuth session to keyring: {e}")

    def save_custom_keys(self, client_id: str, client_secret: str):
        try:
            keyring.set_password(KEYRING_SERVICE, CLIENT_ID_KEY, client_id)
            keyring.set_password(KEYRING_SERVICE, CLIENT_SECRET_KEY, client_secret)
            logger.info("Custom API keys saved to system keyring")
        except Exception as e:
            logger.error(f"Failed to save custom keys to keyring: {e}")

    def get_custom_keys(self):
        try:
            client_id = keyring.get_password(KEYRING_SERVICE, CLIENT_ID_KEY)
            client_secret = keyring.get_password(KEYRING_SERVICE, CLIENT_SECRET_KEY)
            return client_id, client_secret
        except Exception as e:
            logger.error(f"Failed to get custom keys from keyring: {e}")
            return None, None

    def get_current_session(self) -> Session:
        if self._session_cache_valid and self._cached_session:
            return self._cached_session

        try:
            oauth_session_json = keyring.get_password(
                KEYRING_SERVICE, OAUTH_SESSION_KEY
            )
            if oauth_session_json:
                data = json.loads(oauth_session_json)
                session = Session.from_dict(data)
                if session.auth_mode == AuthMode.OAUTH and session.jwt_token:
                    logger.info(
                        f"Found active OAuth session for user '{session.username}'"
                    )
                    self._cached_session = session
                    self._session_cache_valid = True
                    return session
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Failed to load OAuth session from keyring: {e}")
            try:
                keyring.delete_password(KEYRING_SERVICE, OAUTH_SESSION_KEY)
            except Exception:
                pass

        try:
            client_id = keyring.get_password(KEYRING_SERVICE, CLIENT_ID_KEY)
            client_secret = keyring.get_password(KEYRING_SERVICE, CLIENT_SECRET_KEY)
            if client_id and client_secret:
                logger.info("Found active session using Custom Keys")
                session = Session(auth_mode=AuthMode.CUSTOM_KEYS)
                self._cached_session = session
                self._session_cache_valid = True
                return session
        except Exception as e:
            logger.error(f"Failed to read from keyring, treating as logged out: {e}")

        logger.info("No active session found")
        session = Session(auth_mode=AuthMode.LOGGED_OUT)
        self._cached_session = session
        self._session_cache_valid = True
        return session

    def clear_session(self):
        logger.info("Clearing all session data...")
        self._cached_session = None
        self._session_cache_valid = False

        try:
            if keyring.get_password(KEYRING_SERVICE, OAUTH_SESSION_KEY):
                keyring.delete_password(KEYRING_SERVICE, OAUTH_SESSION_KEY)
                logger.debug("OAuth session deleted from keyring")
        except Exception as e:
            logger.warning(f"Could not delete OAuth session from keyring: {e}")

        try:
            if keyring.get_password(KEYRING_SERVICE, CLIENT_ID_KEY):
                keyring.delete_password(KEYRING_SERVICE, CLIENT_ID_KEY)
            if keyring.get_password(KEYRING_SERVICE, CLIENT_SECRET_KEY):
                keyring.delete_password(KEYRING_SERVICE, CLIENT_SECRET_KEY)
            logger.debug("Custom keys deleted from keyring")
        except Exception as e:
            logger.warning(
                f"Could not delete keys from keyring (they might not exist): {e}"
            )

    def clear_oauth_session_only(self):
        logger.info("Clearing OAuth session only...")
        self._cached_session = None
        self._session_cache_valid = False

        try:
            if keyring.get_password(KEYRING_SERVICE, OAUTH_SESSION_KEY):
                keyring.delete_password(KEYRING_SERVICE, OAUTH_SESSION_KEY)
                logger.debug("OAuth session deleted from keyring")
        except Exception as e:
            logger.warning(f"Could not delete OAuth session from keyring: {e}")
