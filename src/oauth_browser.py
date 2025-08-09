import logging
import socket
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from auth_manager import AuthManager
from app_config import FRONTEND_BASE_URL

logger = logging.getLogger(__name__)


class BrowserOAuthFlow:
    def __init__(self, auth_manager: AuthManager):
        self.auth_manager = auth_manager
        self.callback_server = None
        self.callback_port = None
        self.received_token: str | None = None
        self.server_error: str | None = None

    def reset_state(self):
        """Reset OAuth browser state for clean logout/login cycle"""
        if self.callback_server:
            try:
                self.callback_server.shutdown()
                self.callback_server.server_close()
            except Exception:
                pass
        self.callback_server = None
        self.callback_port = None
        self.received_token = None
        self.server_error = None

    def _find_free_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            s.listen(1)
            port = s.getsockname()[1]
        return port

    def _create_callback_handler(self):
        flow = self

        class CallbackHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    parsed_url = urlparse(self.path)
                    query_params = parse_qs(parsed_url.query)

                    if (
                        "jwt_token" in query_params
                        and "user_id" in query_params
                        and "username" in query_params
                    ):
                        jwt_token = query_params["jwt_token"][0]
                        user_id = int(query_params["user_id"][0])
                        username = query_params["username"][0]

                        flow.auth_manager.save_oauth_session(
                            jwt_token, user_id, username
                        )
                        flow.received_token = jwt_token

                        # Redirect to frontend success page with JWT token
                        frontend_url = (
                            f"{FRONTEND_BASE_URL}/oauth/success"
                            f"?jwt_token={jwt_token}&username={username}"
                            f"&user_id={user_id}&source=desktop"
                        )

                        self.send_response(302)
                        self.send_header("Location", frontend_url)
                        self.end_headers()

                        logger.info(f"OAuth callback received for user '{username}'")
                    elif "error" in query_params:
                        error = query_params["error"][0]
                        flow.server_error = error

                        error_url = f"{FRONTEND_BASE_URL}/oauth/success?error={error}&source=desktop"
                        self.send_response(302)
                        self.send_header("Location", error_url)
                        self.end_headers()

                        logger.error(f"OAuth callback error: {error}")
                    else:
                        invalid_url = f"{FRONTEND_BASE_URL}/oauth/success?error=invalid_callback&source=desktop"
                        self.send_response(302)
                        self.send_header("Location", invalid_url)
                        self.end_headers()

                except Exception as e:
                    flow.server_error = str(e)
                    logger.error(f"Error handling OAuth callback: {e}")
                    self.send_response(500)
                    self.end_headers()

            def log_message(self, format, *args):
                pass

        return CallbackHandler

    def start_login(self):
        try:
            self.callback_port = self._find_free_port()

            handler_class = self._create_callback_handler()
            self.callback_server = HTTPServer(
                ("localhost", self.callback_port), handler_class
            )

            server_thread = threading.Thread(
                target=self.callback_server.serve_forever, daemon=True
            )
            server_thread.start()

            login_url = self.auth_manager.get_oauth_login_url(self.callback_port)
            logger.info(f"Starting OAuth callback server on port {self.callback_port}")
            logger.info(f"Opening browser for OAuth login: {login_url}")
            webbrowser.open(login_url)
            return True
        except Exception as e:
            logger.error(f"Failed to start OAuth flow: {e}")
            return False

    def wait_for_session(self, timeout_seconds=60):
        logger.info(f"Waiting for OAuth callback (timeout: {timeout_seconds}s)...")

        start_time = time.time()
        check_interval = 0.1

        try:
            while time.time() - start_time < timeout_seconds:
                if self.received_token:
                    session = self.auth_manager.get_current_session()
                    username = (
                        session.username if session and session.username else "Unknown"
                    )
                    logger.info(f"OAuth session received for user '{username}'")
                    return session
                elif self.server_error:
                    logger.error(f"OAuth callback error: {self.server_error}")
                    return None

                time.sleep(check_interval)

            logger.warning("OAuth session timeout - no callback received")
            return None
        finally:
            if self.callback_server:
                self.callback_server.shutdown()
                self.callback_server.server_close()
                logger.info("OAuth callback server stopped")
