import sqlite3
import os
import logging
import threading
from config import DB_FILE

if not os.path.isabs(DB_FILE):
    from utils import get_resource_path

    DB_FILE = get_resource_path(DB_FILE.replace("../", ""))

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
                    logger.info("Database initialized: %s", os.path.normpath(DB_FILE))
                except Exception as e:
                    logger.error("Error initializing database: %s", e)
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
                except Exception as e:
                    logger.error("Error closing database connection: %s", e)


db_manager = DatabaseManager()


def db_init():
    try:
        db_manager.initialize()
    except Exception as e:
        logger.error("Error initializing database: %s", e)
        raise


def db_save(bid, status, artist, title, version, creator, objs):
    try:
        conn = db_manager.get_connection()
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO beatmap_info (
                    beatmap_id, status, artist, title, version, creator, hit_objects
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (str(bid), status, artist, title, version, creator, objs))
    except Exception as e:
        logger.error("Error saving data to database: %s", e)


def db_get(bid):
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT status, artist, title, version, creator, hit_objects
            FROM beatmap_info
            WHERE beatmap_id=?
        """, (str(bid),))
        row = cursor.fetchone()
        cursor.close()

        if row:
            return {
                "status": row[0],
                "artist": row[1],
                "title": row[2],
                "version": row[3],
                "creator": row[4],
                "hit_objects": row[5]
            }
        return None
    except Exception as e:
        logger.error("Error retrieving data from database: %s", e)
        return None


def db_close():
    db_manager.close()
