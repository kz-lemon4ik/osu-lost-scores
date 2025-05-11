import sqlite3
import os
import logging
import threading
from config import DB_FILE
from utils import mask_path_for_log

logger = logging.getLogger(__name__)


class DatabaseManager:
    _instance = None
    _lock = threading.Lock()
    _conn = None
    _initialized = False

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseManager, cls).__new__(cls)
            return cls._instance

    def initialize(self):
        with self._lock:
            if not self._initialized:
                try:
                    self._conn = sqlite3.connect(DB_FILE, check_same_thread=False)

                    self._conn.execute("PRAGMA foreign_keys = ON")
                    self._conn.execute("PRAGMA synchronous = NORMAL")
                    self._conn.execute("PRAGMA journal_mode = WAL")

                    with self._conn:
                        self._conn.execute("""
                            CREATE TABLE IF NOT EXISTS beatmap_info (
                                beatmap_id TEXT PRIMARY KEY,
                                status TEXT,
                                artist TEXT,
                                title TEXT,
                                version TEXT,
                                creator TEXT,
                                hit_objects INT
                            )
                        """)

                    self._initialized = True
                    logger.info(
                        "Database initialized: %s",
                        mask_path_for_log(os.path.normpath(DB_FILE)),
                    )
                except Exception:
                    logger.exception("Error initializing database:")
                    raise

    def get_connection(self):
        if not self._initialized:
            self.initialize()
        return self._conn

    def close(self):
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                    self._conn = None
                    self._initialized = False
                    logger.info("Database connection closed")
                except Exception:
                    logger.exception("Error closing database connection:")


db_manager = DatabaseManager()
db_operation_lock = threading.Lock()


def db_init():
    try:
        db_manager.initialize()
    except Exception:
        logger.exception("Error initializing database:")
        raise


def db_save(bid, status, artist, title, version, creator, objs):
    try:
                                                          
        with db_operation_lock:
            conn = db_manager.get_connection()
            with conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO beatmap_info (
                        beatmap_id, status, artist, title, version, creator, hit_objects
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (str(bid), status, artist, title, version, creator, objs),
                )
    except Exception:
        logger.exception("Error saving data to database:")


def db_get(bid):
    try:
                                                                                      
                                                                                    
        with db_operation_lock:
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT status, artist, title, version, creator, hit_objects
                FROM beatmap_info
                WHERE beatmap_id=?
            """,
                (str(bid),),
            )
            row = cursor.fetchone()
            cursor.close()

            if row:
                return {
                    "status": row[0],
                    "artist": row[1],
                    "title": row[2],
                    "version": row[3],
                    "creator": row[4],
                    "hit_objects": row[5],
                }
            return None
    except Exception:
        logger.exception("Error retrieving data from database:")
        return None


def db_close():
    db_manager.close()
