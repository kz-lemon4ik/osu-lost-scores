
import datetime
import hashlib
import logging
import os
import struct
import threading

import rosu_pp_py as rosu

from app_config import CACHE_DIR, IO_THREAD_POOL_SIZE, MAPS_DIR
from database import db_get_map, db_manager, db_read_lock, db_upsert_from_scan
from path_utils import mask_path_for_log, get_project_root
from utils import process_in_batches

logger = logging.getLogger(__name__)
asset_downloads_logger = logging.getLogger("asset_downloads")
replay_processing_details_logger = logging.getLogger("replay_processing_details")

os.makedirs(CACHE_DIR, exist_ok=True)

class FileParser:
    
    def __init__(self):
        self.osu_base_path = None
        self.beatmap_id_to_path_map = {}
        self.beatmap_id_to_path_lock = threading.Lock()
        os.makedirs(MAPS_DIR, exist_ok=True)
        self.file_access_lock = threading.Lock()

    def set_osu_base_path(self, path):
        if path:
            self.osu_base_path = os.path.normpath(path)
            logger.info(
                f"osu! base path set to: {mask_path_for_log(self.osu_base_path)}"
            )

    def to_relative_path(self, abs_path):
        if not abs_path:
            return None

        norm_path = os.path.normpath(abs_path)

        if self.osu_base_path and norm_path.startswith(self.osu_base_path):
            return os.path.relpath(norm_path, self.osu_base_path)

        try:
            project_root = os.path.normpath(get_project_root())
            if norm_path.startswith(project_root):
                return os.path.relpath(norm_path, project_root)
        except (TypeError, AttributeError):
            pass

        return abs_path

    def to_absolute_path(self, rel_path):
        if not rel_path or os.path.isabs(rel_path):
            return rel_path

        if self.osu_base_path:
            abs_path_game = os.path.normpath(os.path.join(self.osu_base_path, rel_path))
            if os.path.exists(abs_path_game):
                return abs_path_game

        try:
            project_root = os.path.normpath(get_project_root())
            abs_path_project = os.path.normpath(os.path.join(project_root, rel_path))
            if os.path.exists(abs_path_project):
                return abs_path_project
        except (TypeError, AttributeError):
            pass

        return rel_path

    def reset_in_memory_caches(self, osu_api_client=None):
        with self.beatmap_id_to_path_lock:
            self.beatmap_id_to_path_map.clear()
        if osu_api_client:
            osu_api_client.reset_caches()
        logger.info("In-memory cache has been reset")

    @staticmethod
    def read_string(data, offset):
        if data[offset] == 0x00:
            return "", offset + 1
        elif data[offset] == 0x0B:
            offset += 1
            length = 0
            shift = 0
            while True:
                byte = data[offset]
                offset += 1
                length |= (byte & 0x7F) << shift
                if not (byte & 0x80):
                    break
                shift += 7
            s = data[offset: offset + length].decode("utf-8", errors="ignore")
            return s, offset + length
        else:
            raise ValueError("Invalid string in .osr")

    MODS_MAPPING_ITER = [
        (1, "NF"),
        (2, "EZ"),
        (8, "HD"),
        (16, "HR"),
        (32, "SD"),
        (64, "DT"),
        (128, "RX"),
        (256, "HT"),
        (512, "NC"),
        (1024, "FL"),
        (4096, "SO"),
        (8192, "AP"),
        (536870912, "SCOREV2"),
    ]
    DISALLOWED_MODS = {"RX", "AT", "AP", "SCOREV2"}

    def parse_mods(self, mods_int):
        mods = []
        if mods_int & 512:
            mods.append("NC")
        if mods_int & 16384:
            mods.append("PF")
        for bit, name in self.MODS_MAPPING_ITER:
            if mods_int & bit:
                mods.append(name.upper())
        return tuple(sorted(set(mods), key=lambda x: x))

    @staticmethod
    def sort_mods(mod_list):
        if not mod_list:
            return []
        priority = {
            "EZ": 1, "HD": 2, "DT": 3, "NC": 3, "HT": 3,
            "HR": 4, "FL": 5, "NF": 6, "SO": 7,
        }
        return sorted(mod_list, key=lambda m: (priority.get(m, 9999), m))

    def parse_osr(self, osr_path):
        with open(osr_path, "rb") as f:
            data = f.read()
        offset = 0
        mode = data[offset]
        offset += 1
        offset += 4
        beatmap_md5, offset = self.read_string(data, offset)
        player, offset = self.read_string(data, offset)
        _, offset = self.read_string(data, offset)
        c300 = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        c100 = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        c50 = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        offset += 2
        offset += 2
        c_miss = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        total = struct.unpack_from("<I", data, offset)[0]
        offset += 4
        max_combo = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        perfect = data[offset]
        offset += 1
        full_combo = perfect == 0x01
        mods_int = struct.unpack_from("<I", data, offset)[0]
        offset += 4
        mods = self.parse_mods(mods_int)
        if any(m in self.DISALLOWED_MODS for m in mods):
            return None
        _, offset = self.read_string(data, offset)
        win_ts = struct.unpack_from("<q", data, offset)[0]
        offset += 8
        ts_ms = win_ts / 10000 - 62135596800000
        ts = int(ts_ms // 1000)
        aware_dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        tstr = aware_dt.strftime("%d-%m-%Y %H-%M-%S")
        return {
            "game_mode": mode,
            "beatmap_md5": beatmap_md5,
            "player_name": player.strip(),
            "count300": c300,
            "count100": c100,
            "count50": c50,
            "countMiss": c_miss,
            "total_score": total,
            "max_combo": max_combo,
            "is_full_combo": full_combo,
            "mods_list": mods,
            "score_timestamp": ts,
            "score_time": tstr,
        }

    @staticmethod
    def calc_acc(c300, c100, c50, c_miss):
        hits = c300 + c100 + c50 + c_miss
        if hits == 0:
            return 100.0
        return round((300 * c300 + 100 * c100 + 50 * c50) / (300 * hits) * 100, 2)

    @staticmethod
    def get_md5(path):
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def find_osu(self, songs_folder, progress_callback=None, gui_log=None, progress_logger=None):
        if gui_log:
            gui_log("Building list of .osu files...", update_last=True)

        files = []
        with self.beatmap_id_to_path_lock:
            self.beatmap_id_to_path_map.clear()

        for root, dirs, filenames in os.walk(songs_folder):
            for file in filenames:
                if file.endswith(".osu"):
                    files.append(os.path.join(root, file))

        if os.path.exists(MAPS_DIR) and os.path.isdir(MAPS_DIR):
            for file in os.listdir(MAPS_DIR):
                if file.endswith(".osu"):
                    files.append(os.path.join(MAPS_DIR, file))

        logger.info(f"Found {len(files)} .osu files. Starting processing...")

        def process_file(file_path):
            try:
                rel_path = self.to_relative_path(file_path)
                existing_record = db_get_map(rel_path, by="path")
                current_mtime = int(os.path.getmtime(file_path))

                if (
                        existing_record
                        and existing_record.get("last_modified") == current_mtime
                ):
                    return

                md5_hash = self.get_md5(file_path)
                metadata = self.parse_osu_metadata(file_path)

                update_data = {
                    "file_path": rel_path,
                    "last_modified": current_mtime,
                    "beatmap_id": metadata.get("beatmap_id"),
                    "beatmapset_id": metadata.get("beatmapset_id"),
                    "artist": metadata.get("artist"),
                    "title": metadata.get("title"),
                    "creator": metadata.get("creator"),
                    "version": metadata.get("version"),
                }

                if not existing_record:
                    update_data["lookup_status"] = "pending"
                    update_data["api_status"] = "unknown"

                db_upsert_from_scan(md5_hash, update_data)

            except Exception as proc_exc:
                replay_processing_details_logger.warning(
                    f"Could not process file {mask_path_for_log(file_path)}: {proc_exc}"
                )

        process_in_batches(
            files,
            batch_size=min(500, len(files)),
            max_workers=IO_THREAD_POOL_SIZE,
            process_func=process_file,
            progress_callback=progress_callback,
            gui_log=gui_log,
            progress_logger=progress_logger,
            log_interval_sec=5,
            progress_message="Processing .osu files",
        )

        logger.info("Building beatmap_id to file path mapping from database...")
        try:
            with db_read_lock:
                conn = db_manager.get_connection()
                if conn is None:
                    logger.error("Failed to get database connection")
                    return
                cursor = conn.cursor()
                # noinspection SqlNoDataSourceInspection
                cursor.execute(
                    "SELECT beatmap_id, file_path FROM maps_cache WHERE beatmap_id IS NOT NULL"
                )
                rows = cursor.fetchall()
                cursor.close()

            with self.beatmap_id_to_path_lock:
                self.beatmap_id_to_path_map.clear()
                for bid, path in rows:
                    abs_path = self.to_absolute_path(path)
                    if abs_path:
                        self.beatmap_id_to_path_map[bid] = abs_path

            logger.info(
                f"Built beatmap_id_to_path map with {len(self.beatmap_id_to_path_map)} entries"
            )
        except Exception as e:
            logger.error(f"Failed to build beatmap_id_to_path map from DB: {e}")

        return None

    def parse_osr_info(self, osr_path, username):
        try:
            rep = self.parse_osr(osr_path)
            if not rep:
                replay_processing_details_logger.warning(
                    "Failed to process osr: %s", mask_path_for_log(osr_path)
                )

                return None
            if rep["game_mode"] != 0:
                return None
            if rep["player_name"].lower() != username.lower():
                return None
            rep["osr_path"] = osr_path
            return rep
        except Exception as e:
            replay_processing_details_logger.exception(
                f"Unexpected error preprocessing replay {mask_path_for_log(osr_path)}: {e}"
            )
            return None

    @staticmethod
    def parse_beatmap_id(osu_path):
        beatmap_id = None
        try:
            with open(osu_path, "r", encoding="utf-8", errors="ignore") as f:
                in_metadata = False
                for line in f:
                    line = line.strip()
                    if line.startswith("[Metadata]"):
                        in_metadata = True
                        continue
                    if in_metadata and line.startswith("[") and line.endswith("]"):
                        break
                    if in_metadata and line.lower().startswith("beatmapid:"):
                        parts = line.split(":", 1)
                        if len(parts) == 2:
                            val = parts[1].strip()
                            if val.isdigit():
                                beatmap_id = int(val)
                        break
        except IOError as e:
            logger.warning("Failed to read beatmap file %s: %s", mask_path_for_log(osu_path), e)
        except (IndexError, ValueError) as e:
            logger.debug("Error parsing beatmap ID from %s: %s", mask_path_for_log(osu_path), e)
        return beatmap_id

    @staticmethod
    def calculate_pp_rosu(osu_path, replay):
        # noinspection PyBroadException
        try:
            beatmap = rosu.Beatmap(path=osu_path)
            acc = FileParser.calc_acc(
                replay["count300"],
                replay["count100"],
                replay["count50"],
                replay["countMiss"],
            )

            mods_string = "".join(FileParser.sort_mods(replay["mods_list"]))

            perf = rosu.Performance(
                lazer=False,
                accuracy=acc,
                combo=replay["max_combo"],
                misses=replay["countMiss"],
                mods=mods_string,
            )
            attrs = perf.calculate(beatmap)

            if not attrs:
                return None

            return {
                "pp": round(float(attrs.pp)),
                "Accuracy": acc,
            }
        except Exception:
            replay_processing_details_logger.exception(
                "Error calculating PP via rosu-pp for %s", mask_path_for_log(osu_path)
            )
            return None

    def process_osr_with_path(self, replay_data, prefetched_data=None):
        if not replay_data:
            return None
        try:
            beatmap_md5 = replay_data.get("beatmap_md5")
            osr_path = replay_data.get("osr_path")
            if not beatmap_md5 or not osr_path:
                return None

            map_data_from_db = db_get_map(beatmap_md5, by="md5")
            if not map_data_from_db or not map_data_from_db.get("file_path"):
                replay_processing_details_logger.warning(
                    f"Could not find osu path for md5 {beatmap_md5} in DB"
                )
                return None

            osu_path = self.to_absolute_path(map_data_from_db["file_path"])
            if not osu_path or not os.path.exists(osu_path):
                return None

            pp_info = self.calculate_pp_rosu(osu_path, replay_data)
            if not pp_info:
                return None

            final_score = {**replay_data, **pp_info, "osu_file_path": osu_path}

            if prefetched_data and isinstance(prefetched_data, dict):
                final_score["beatmap_id"] = prefetched_data.get("id")
                bset = prefetched_data.get("beatmapset", {})
                final_score["artist"] = bset.get("artist")
                final_score["title"] = bset.get("title")
                final_score["creator"] = bset.get("creator")
                final_score["version"] = prefetched_data.get("version")

            if not final_score.get("beatmap_id"):
                final_score["beatmap_id"] = self.parse_beatmap_id(osu_path)

            if not all(
                    k in final_score and final_score[k]
                    for k in ["artist", "title", "creator", "version"]
            ):
                file_meta = self.parse_osu_metadata(osu_path)
                if not final_score.get("artist"):
                    final_score["artist"] = file_meta.get("artist")
                if not final_score.get("title"):
                    final_score["title"] = file_meta.get("title")
                if not final_score.get("creator"):
                    final_score["creator"] = file_meta.get("creator")
                if not final_score.get("version"):
                    final_score["version"] = file_meta.get("version")

            final_score["mods"] = final_score.pop("mods_list", [])

            return final_score
        except Exception as e:
            logger.exception(f"Unexpected error processing replay with path: {e}")
            return None

    def count_objs(self, osu_path, beatmap_id):
        map_data = db_get_map(beatmap_id, by="id")

        if map_data and map_data.get("hit_objects") is not None:
            replay_processing_details_logger.debug(
                f"Using cached hit_objects ({map_data['hit_objects']}) from DB for beatmap_id {beatmap_id}"
            )
            return map_data["hit_objects"]

        total = 0

        if not osu_path or not os.path.exists(osu_path):
            if map_data and map_data.get("file_path"):
                osu_path = self.to_absolute_path(map_data.get("file_path"))
            else:
                replay_processing_details_logger.warning(
                    f"Cannot count objects: file path for beatmap_id {beatmap_id} is unknown"
                )
                return 0

        try:
            with open(osu_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

            hit_objects_pos = content.find("[HitObjects]")
            if hit_objects_pos != -1:
                section_text = content[hit_objects_pos + len("[HitObjects]"):]
                next_section_pos = section_text.find("\n[")
                if next_section_pos != -1:
                    section_text = section_text[:next_section_pos]

                lines = section_text.strip().split("\n")
                total = sum(
                    1
                    for line in lines
                    if line.strip() and not line.strip().startswith("//")
                )

            if map_data and map_data.get("md5_hash"):
                db_upsert_from_scan(map_data["md5_hash"], {"hit_objects": total})
                replay_processing_details_logger.debug(
                    f"Locally counted and saved {total} objects to DB for beatmap_id {beatmap_id}"
                )
            else:
                replay_processing_details_logger.warning(
                    f"Could not save hit_objects count for beatmap_id {beatmap_id} as md5_hash is unknown"
                )

        except Exception as e:
            replay_processing_details_logger.error(
                "Error reading .osu file %s: %s", mask_path_for_log(osu_path), e
            )
            return 0

        return total

    def parse_osu_metadata(self, osu_path):
        result = {
            "artist": "",
            "title": "",
            "creator": "",
            "version": "",
            "beatmapset_id": None,
        }
        try:
            with self.file_access_lock:
                if not os.path.exists(osu_path):
                    replay_processing_details_logger.warning(
                        f"File not found: {mask_path_for_log(osu_path)}"
                    )
                    return result
                with open(osu_path, "r", encoding="utf-8", errors="ignore") as f:
                    in_metadata = False
                    for line in f:
                        line = line.strip()
                        if line.startswith("[Metadata]"):
                            in_metadata = True
                            continue
                        if in_metadata and line.startswith("[") and line.endswith("]"):
                            break
                        if in_metadata:
                            if line.lower().startswith("artist:"):
                                parts = line.split(":", 1)
                                if len(parts) == 2:
                                    result["artist"] = parts[1].strip()
                            elif line.lower().startswith("title:"):
                                parts = line.split(":", 1)
                                if len(parts) == 2:
                                    result["title"] = parts[1].strip()
                            elif line.lower().startswith("creator:"):
                                parts = line.split(":", 1)
                                if len(parts) == 2:
                                    result["creator"] = parts[1].strip()
                            elif line.lower().startswith("version:"):
                                parts = line.split(":", 1)
                                if len(parts) == 2:
                                    result["version"] = parts[1].strip()
                            elif line.lower().startswith("beatmapid:"):
                                parts = line.split(":", 1)
                                if len(parts) == 2:
                                    val = parts[1].strip()
                                    if val.isdigit():
                                        result["beatmap_id"] = int(val)
                            elif line.lower().startswith("beatmapsetid:"):
                                parts = line.split(":", 1)
                                if len(parts) == 2:
                                    bset_id = parts[1].strip()
                                    if bset_id.isdigit():
                                        result["beatmapset_id"] = bset_id
        except Exception as e:
            replay_processing_details_logger.exception(
                "Error parsing .osu file %s: %s", mask_path_for_log(osu_path), e
            )
        return result

    def grade_osu(self, beatmap_id, c300, c50, c_miss, osu_file_path=None):
        db_info = db_get_map(beatmap_id, by="id")
        total = 0
        if db_info:
            total = db_info.get("hit_objects") or 0
            if total > 0:
                replay_processing_details_logger.debug(
                    f"Using hit_objects ({total}) from DB for grade calculation, beatmap_id: {beatmap_id}"
                )
        if not total:
            osu_file = osu_file_path
            if osu_file:
                replay_processing_details_logger.debug(
                    f"For beatmap_id {beatmap_id} using provided path: {mask_path_for_log(osu_file)}"
                )
            if not osu_file:
                with self.beatmap_id_to_path_lock:
                    osu_file = self.beatmap_id_to_path_map.get(beatmap_id)
                    if osu_file:
                        replay_processing_details_logger.debug(
                            f"For beatmap_id {beatmap_id} found path in beatmap_id_to_path_map: {mask_path_for_log(osu_file)}"
                        )
            if osu_file:
                total = self.count_objs(osu_file, beatmap_id)
            if not total:
                replay_processing_details_logger.warning(
                    f"Failed to determine object count for beatmap_id {beatmap_id}"
                )
                return "?"
        c300_corrected = c300
        p300 = (c300_corrected / total) * 100 if total else 0
        p50 = (c50 / total) * 100 if total else 0
        if p300 == 100:
            rank = "SS"
        elif p300 > 90 and p50 <= 1 and c_miss == 0:
            rank = "S"
        elif p300 > 90:
            rank = "A"
        elif p300 > 80 and c_miss == 0:
            rank = "A"
        elif p300 > 80:
            rank = "B"
        elif p300 > 70 and c_miss == 0:
            rank = "B"
        elif p300 > 60:
            rank = "C"
        else:
            rank = "D"
        replay_processing_details_logger.debug(
            f"Grade for beatmap_id {beatmap_id}: {rank} (p300: {p300:.2f}%, p50: {p50:.2f}%, hits: {total})"
        )
        return rank

    def get_calc_acc(self):
        return self.calc_acc

file_parser = FileParser()
