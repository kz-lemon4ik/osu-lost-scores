import sqlite3
import os
import logging
from config import DB_FILE

                       
if not os.path.isabs(DB_FILE):
    DB_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), DB_FILE))

def db_init():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("""
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
            conn.commit()
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error initializing database: {e}")
        raise

def db_save(bid, status, artist, title, version, creator, objs):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO beatmap_info (
                    beatmap_id, status, artist, title, version, creator, hit_objects
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (str(bid), status, artist, title, version, creator, objs))
            conn.commit()
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error saving data to database: {e}")

def db_get(bid):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT status, artist, title, version, creator, hit_objects
                FROM beatmap_info
                WHERE beatmap_id=?
            """, (str(bid),))
            row = cur.fetchone()

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
        logger = logging.getLogger(__name__)
        logger.error(f"Error retrieving data from database: {e}")
        return None