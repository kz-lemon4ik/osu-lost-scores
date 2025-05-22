import logging
import os
import sqlite3
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
                                                         
                        cursor = self._conn.cursor()
                        cursor.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name='beatmap_info'"
                        )
                        table_exists = cursor.fetchone() is not None

                        if not table_exists:
                                                                        
                            self._conn.execute(
                                """
                                CREATE TABLE IF NOT EXISTS beatmap_info (
                                    beatmap_id TEXT PRIMARY KEY,
                                    status TEXT,
                                    artist TEXT,
                                    title TEXT,
                                    version TEXT,
                                    creator TEXT,
                                    hit_objects INT,
                                    beatmapset_id TEXT,
                                    last_updated INTEGER
                                )
                            """
                            )
                        else:
                                                                     
                            cursor.execute("PRAGMA table_info(beatmap_info)")
                            columns = [col[1] for col in cursor.fetchall()]

                            if "last_updated" not in columns:
                                                                             
                                self._conn.execute(
                                    "ALTER TABLE beatmap_info ADD COLUMN last_updated INTEGER"
                                )
                                logger.info(
                                    "Added last_updated column to beatmap_info table"
                                )

                    self._initialized = True
                    logger.debug(
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
db_read_lock = threading.RLock()                                            
db_write_lock = threading.Lock()                                            


def db_init():
    try:
        db_manager.initialize()
    except Exception:
        logger.exception("Error initializing database:")
        raise


def db_save(bid, status, artist, title, version, creator, objs, bset_id=None):
    try:
        import time

        current_timestamp = int(time.time())

        with db_write_lock:                                    
            conn = db_manager.get_connection()
            with conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO beatmap_info (
                        beatmap_id, status, artist, title, version, creator, hit_objects, beatmapset_id, last_updated
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        str(bid),
                        status,
                        artist,
                        title,
                        version,
                        creator,
                        objs,
                        bset_id,
                        current_timestamp,
                    ),
                )
    except Exception:
        logger.exception("Error saving data to database:")


def db_get(bid):
    try:
        with db_read_lock:                                                 
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT status, artist, title, version, creator, hit_objects, beatmapset_id
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
                    "beatmapset_id": row[6],
                }
            return None
    except Exception:
        logger.exception("Error retrieving data from database:")
        return None


def db_get_field_with_fallback(bid, field_name, default_value=None):
           
    try:
        with db_read_lock:
            conn = db_manager.get_connection()
            cursor = conn.cursor()

                                            
            cursor.execute("PRAGMA table_info(beatmap_info)")
            columns = [col[1] for col in cursor.fetchall()]

            if field_name not in columns:
                logger.warning(
                    f"Field '{field_name}' does not exist in beatmap_info table"
                )
                return default_value

            cursor.execute(
                f"SELECT {field_name} FROM beatmap_info WHERE beatmap_id=?",
                (str(bid),),
            )
            row = cursor.fetchone()
            cursor.close()

            if row:
                return row[0]
            return default_value
    except Exception:
        logger.exception(f"Error retrieving field {field_name} from database:")
        return default_value


def db_close():
    db_manager.close()
