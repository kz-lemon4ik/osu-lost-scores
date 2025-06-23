
import logging
import os
import sqlite3
import threading

from app_config import DB_FILE
from path_utils import mask_path_for_log

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

    # noinspection SqlNoDataSourceInspection
    def initialize(self):
        
        with self._lock:
            if not self._initialized:
                try:
                    self._conn = sqlite3.connect(
                        DB_FILE, check_same_thread=False, isolation_level=None
                    )
                    self._conn.execute("PRAGMA foreign_keys = ON")
                    self._conn.execute("PRAGMA synchronous = NORMAL")
                    self._conn.execute("PRAGMA journal_mode = WAL")
                    with self._conn:
                        cursor = self._conn.cursor()
                        cursor.execute(
                            """CREATE TABLE IF NOT EXISTS maps_cache (
                                md5_hash TEXT PRIMARY KEY,
                                file_path TEXT,
                                last_modified INTEGER,
                                beatmap_id INTEGER,
                                beatmapset_id INTEGER,
                                lookup_status TEXT,
                                api_status TEXT,
                                artist TEXT,
                                title TEXT,
                                creator TEXT,
                                version TEXT,
                                hit_objects INTEGER,
                                last_updated INTEGER DEFAULT 0
                            );"""
                        )
                        cursor.execute(
                            "CREATE INDEX IF NOT EXISTS idx_beatmap_id ON maps_cache (beatmap_id);"
                        )
                    self._initialized = True
                    logger.debug(
                        "Database initialized: %s",
                        mask_path_for_log(os.path.normpath(DB_FILE)),
                    )
                except sqlite3.Error as e:
                    logger.exception("Error initializing database: %s", e)
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
                except sqlite3.Error:
                    logger.exception("Error closing database connection:")

db_manager = DatabaseManager()
db_read_lock = threading.RLock()  # Reentrant lock for read operations
db_write_lock = threading.Lock()  # Exclusive lock for write operations

def db_init():
    
    db_manager.initialize()

def db_close():
    
    db_manager.close()

def db_get_map(identifier, by="md5"):
    
    if not identifier:
        return None
    try:
        with db_read_lock:
            conn = db_manager.get_connection()
            if conn is None:
                logger.error("Failed to get database connection")
                return None
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            if by == "id":
                query_col = "beatmap_id"
            elif by == "path":
                query_col = "file_path"
            else:
                query_col = "md5_hash"

            # noinspection SqlNoDataSourceInspection
            cursor.execute(
                f"SELECT * FROM maps_cache WHERE {query_col} = ? LIMIT 1", (identifier,)
            )

            row = cursor.fetchone()
            cursor.close()
            if conn is not None:
                conn.row_factory = None
            return dict(row) if row else None
    except sqlite3.Error as e:
        logger.exception("Error retrieving data from database: %s", e)
    except TypeError as e:
        logger.error("Invalid identifier type: %s", e)
        return None

def db_update_from_api(beatmap_id, data_dict):
    
    if not beatmap_id:
        return

    valid_keys = [
        "beatmapset_id",
        "api_status",
        "artist",
        "title",
        "creator",
        "version",
        "hit_objects",
    ]
    filtered_data = {
        k: v for k, v in data_dict.items() if k in valid_keys and v is not None
    }
    if not filtered_data:
        return

    set_clause = ", ".join(f"{key} = ?" for key in filtered_data)
    params = list(filtered_data.values()) + [beatmap_id]

    try:
        with db_write_lock:
            conn = db_manager.get_connection()
            if conn is None:
                logger.error("Failed to get database connection")
                return
            with conn:
                # noinspection SqlNoDataSourceInspection
                conn.execute(
                    f"UPDATE maps_cache SET {set_clause} WHERE beatmap_id = ?", params
                )
    except sqlite3.Error as e:
        logger.exception("Error updating data by beatmap_id %s: %s", beatmap_id, e)

# noinspection SqlNoDataSourceInspection
def db_upsert_from_scan(md5_hash, data_dict):
    
    if not md5_hash:
        return

    try:
        with db_write_lock:
            conn = db_manager.get_connection()
            if conn is None:
                logger.error("Failed to get database connection")
                return
            with conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT md5_hash FROM maps_cache WHERE md5_hash = ?", (md5_hash,)
                )
                row = cursor.fetchone()

                valid_keys = [
                    "file_path", "last_modified", "beatmap_id", "beatmapset_id",
                    "lookup_status", "api_status", "artist", "title", "creator",
                    "version", "hit_objects",
                ]
                filtered_data = {
                    k: v for k, v in data_dict.items() if k in valid_keys and v is not None
                }
                if not filtered_data:
                    cursor.close()
                    return

                if row:
                    set_clause = ", ".join(f"{key} = ?" for key in filtered_data)
                    params = list(filtered_data.values()) + [md5_hash]
                    cursor.execute(
                        f"UPDATE maps_cache SET {set_clause} WHERE md5_hash = ?", params
                    )
                else:
                    filtered_data["md5_hash"] = md5_hash
                    keys = list(filtered_data.keys())
                    placeholders = ", ".join(["?"] * len(keys))
                    values = list(filtered_data.values())
                    cursor.execute(
                        f"INSERT INTO maps_cache ({', '.join(keys)}) VALUES ({placeholders})",
                        values,
                    )
                cursor.close()
    except sqlite3.Error as e:
        logger.exception("Error upserting data for md5 %s: %s", md5_hash, e)
