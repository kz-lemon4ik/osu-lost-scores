import hashlib
import json
import struct
import threading
import datetime
import os
import logging
import rosu_pp_py as rosu
import requests
from database import db_get, db_save
from utils import mask_path_for_log, process_in_batches
from config import MAP_DOWNLOAD_TIMEOUT, MAPS_DIR, CACHE_DIR, IO_THREAD_POOL_SIZE

logger = logging.getLogger(__name__)
os.makedirs(CACHE_DIR, exist_ok=True)


class FileParser:
    def __init__(self):
        self.osu_base_path = None
        self.md5_cache_lock = threading.Lock()
        self.md5_beatmapid_cache = {}
        self.md5_map = {}

        self.osr_cache_path = os.path.join(CACHE_DIR, "osr_cache.json")
        self.md5_cache_path = os.path.join(CACHE_DIR, "osu_md5_cache.json")

        self.osr_cache = {}
        self.osr_cache_lock = threading.Lock()

        self.not_submitted_cache_path = os.path.join(
            CACHE_DIR, "not_submitted_cache.json"
        )
        self.not_submitted_cache = self.not_submitted_cache_load()

                                                             
        os.makedirs(MAPS_DIR, exist_ok=True)

                                               
        self.file_access_lock = threading.Lock()

                                         
    def set_osu_base_path(self, path):
        if path:
            self.osu_base_path = os.path.normpath(path)
            logger.info(
                f"osu! base path set to: {mask_path_for_log(self.osu_base_path)}"
            )

    def to_relative_path(self, abs_path):
        if not abs_path or not self.osu_base_path:
            return abs_path
        try:
                                                               
            if os.path.normpath(abs_path).startswith(self.osu_base_path):
                rel_path = os.path.relpath(abs_path, self.osu_base_path)
                return rel_path
            return abs_path
        except Exception as e:
            logger.error(f"Error converting to relative path: {e}")
            return abs_path

    def to_absolute_path(self, rel_path):
        if not rel_path or not self.osu_base_path:
            return rel_path
        try:
                                                       
            if not os.path.isabs(rel_path):
                abs_path = os.path.normpath(os.path.join(self.osu_base_path, rel_path))
                return abs_path
            return rel_path
        except Exception as e:
            logger.error(f"Error converting to absolute path: {e}")
            return rel_path

    def reset_in_memory_caches(self, osu_api_client=None):
        with self.md5_cache_lock:
            self.md5_beatmapid_cache.clear()
            self.md5_map.clear()
        with self.osr_cache_lock:
            self.osr_cache.clear()
        self.not_submitted_cache.clear()

                                                                 
        if osu_api_client:
            osu_api_client.reset_caches()

        logger.info("In-memory caches (MD5, OSR, NotSubmitted) have been reset.")

    def not_submitted_cache_load(self):
        if os.path.exists(self.not_submitted_cache_path):
            try:
                with open(self.not_submitted_cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning("Error reading not_submitted_cache: %s", e)
                return {}
        return {}

    def not_submitted_cache_save(self, cache):
        try:
            with open(self.not_submitted_cache_path, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=4)
        except Exception as e:
            logger.error("Error writing not_submitted_cache: %s", e)

                                                                         
    def read_string(self, data, offset):
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
            s = data[offset : offset + length].decode("utf-8", errors="ignore")
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

    def sort_mods(self, mod_list):
        priority = {
            "EZ": 1,
            "HD": 2,
            "DT": 3,
            "NC": 3,
            "HT": 3,
            "HR": 4,
            "FL": 5,
            "NF": 6,
            "SO": 7,
        }
        out = [m for m in mod_list if m != "CL"]
        out.sort(key=lambda m: (priority.get(m, 9999), m))
        return out

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
        cMiss = struct.unpack_from("<H", data, offset)[0]
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
        tstr = datetime.datetime.utcfromtimestamp(ts).strftime("%d-%m-%Y %H-%M-%S")

        return {
            "game_mode": mode,
            "beatmap_md5": beatmap_md5,
            "player_name": player.strip(),
            "count300": c300,
            "count100": c100,
            "count50": c50,
            "countMiss": cMiss,
            "total_score": total,
            "max_combo": max_combo,
            "is_full_combo": full_combo,
            "mods_list": mods,
            "score_timestamp": ts,
            "score_time": tstr,
        }

    def calc_acc(self, c300, c100, c50, cmiss):
        hits = c300 + c100 + c50 + cmiss
        if hits == 0:
            return 100.0
        return round((300 * c300 + 100 * c100 + 50 * c50) / (300 * hits) * 100, 2)

    def osr_load(self):
        with self.osr_cache_lock:
            if os.path.exists(self.osr_cache_path):
                try:
                    with open(self.osr_cache_path, "r", encoding="utf-8") as f:
                        content = f.read()
                        logger.debug(
                            "OSR-cache (%s): %s",
                            mask_path_for_log(self.osr_cache_path),
                            content[:200],
                        )
                        f.seek(0)
                        return json.load(f)
                except Exception:
                    logger.exception(
                        "Error reading OSR-cache: %s",
                        mask_path_for_log(self.osr_cache_path),
                    )
            return {}

    def osr_save(self, cache):
        with self.osr_cache_lock:
            try:
                with open(self.osr_cache_path, "w", encoding="utf-8") as f:
                    json.dump(cache, f, indent=4)
            except Exception as e:
                logger.error("Error saving OSR-cache: %s", e)

    def md5_load(self):
        with self.md5_cache_lock:
            if os.path.exists(self.md5_cache_path):
                try:
                    with open(self.md5_cache_path, "r", encoding="utf-8") as f:
                        return json.load(f)
                except Exception as e:
                    logger.warning("Error reading MD5-cache: %s", e)
                    return {}
            return {}

    def md5_save(self, cache):
        with self.md5_cache_lock:
            try:
                with open(self.md5_cache_path, "w", encoding="utf-8") as f:
                    json.dump(cache, f, indent=4)
            except Exception as e:
                logger.error("Error saving MD5-cache: %s", e)

    def get_md5(self, path):
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def find_md5(self, full_path, cache):
        try:
            mtime = os.path.getmtime(full_path)
        except Exception:
            return None

                                                                         
        rel_path = self.to_relative_path(full_path)

                                                                
        if rel_path in cache:
            saved = cache[rel_path]
            if saved.get("mtime") == mtime:
                return saved.get("md5")

                                                                                 
        if full_path in cache:
            saved = cache[full_path]
            if saved.get("mtime") == mtime:
                                                                       
                cache[rel_path] = saved
                                                   
                del cache[full_path]
                return saved.get("md5")

        md5_hash = self.get_md5(full_path)
        cache[rel_path] = {"mtime": mtime, "md5": md5_hash}
        return md5_hash

    def find_osu(self, songs_folder, progress_callback=None):
        files = []
                           
        for root, dirs, filenames in os.walk(songs_folder):
            for file in filenames:
                if file.endswith(".osu"):
                    files.append(os.path.join(root, file))

                                                 
        if os.path.exists(MAPS_DIR) and os.path.isdir(MAPS_DIR):
            for file in os.listdir(MAPS_DIR):
                if file.endswith(".osu"):
                    files.append(os.path.join(MAPS_DIR, file))

            logger.info(
                f"Added {len(os.listdir(MAPS_DIR))} files from MAPS_DIR to scanning"
            )
        total = len(files)
        md5_map = {}
        cache = self.md5_load()

        def process_file(p):
            val = self.find_md5(p, cache)
            return (val, p)

                                              
        results = process_in_batches(
            files,
            batch_size=min(500, len(files)),
            max_workers=IO_THREAD_POOL_SIZE,
            process_func=process_file,
            progress_callback=progress_callback,
        )

                                 
        for res in results:
            if res and res[0]:
                md5_map[res[0]] = res[1]

        self.md5_save(cache)

                                                                   
        self.md5_map = md5_map

        logger.info(f"Total .osu files indexed: {len(md5_map)}")

        return md5_map

    def parse_osr_info(self, osr_path, username):
        try:
            rep = self.parse_osr(osr_path)
            if not rep:
                logger.warning("Failed to process osr: %s", mask_path_for_log(osr_path))
                return None
            if rep["game_mode"] != 0:
                return None
            if rep["player_name"].lower() != username.lower():
                return None

                                                         
            rep["osr_path"] = osr_path
            return rep
        except Exception as e:
            logger.exception(
                f"Unexpected error preprocessing replay {mask_path_for_log(osr_path)}: {e}"
            )
            return None

    def parse_beatmap_id(self, osu_path):
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
        except Exception:
            pass

        return beatmap_id

    def calculate_pp_rosu(self, osu_path, replay):
        try:
            beatmap = rosu.Beatmap(path=osu_path)
            acc = self.calc_acc(
                replay["count300"],
                replay["count100"],
                replay["count50"],
                replay["countMiss"],
            )

            original_mods = replay["mods_list"]

            mods_for_perf = list(original_mods)
            if "CL" not in mods_for_perf:
                mods_for_perf.append("CL")

            priority = {
                "EZ": 1,
                "HD": 2,
                "DT": 3,
                "NC": 3,
                "HT": 3,
                "HR": 4,
                "FL": 5,
                "NF": 6,
                "SO": 7,
            }
            sorted_mods_perf = sorted(
                mods_for_perf, key=lambda m: (priority.get(m, 9999), m)
            )
            mods_string = "".join(sorted_mods_perf)

            perf = rosu.Performance(
                accuracy=acc,
                combo=replay["max_combo"],
                misses=replay["countMiss"],
                mods=mods_string,
            )
            attrs = perf.calculate(beatmap)
            if not attrs:
                return None

            bm_id = self.parse_beatmap_id(osu_path)
            meta = self.parse_osu_metadata(osu_path)

            mods_for_output = [m for m in original_mods if m != "CL"]
            if not mods_for_output:
                mods_for_output = ["NM"]

            result = {
                "pp": round(float(attrs.pp)),
                "beatmap_id": bm_id if bm_id is not None else None,
                "artist": meta["artist"],
                "title": meta["title"],
                "creator": meta["creator"],
                "version": meta["version"],
                "total_score": replay["total_score"],
                "mods": tuple(mods_for_output),
                "count100": replay["count100"],
                "count50": replay["count50"],
                "countMiss": replay["countMiss"],
                "count300": replay["count300"],
                "osu_file_path": mask_path_for_log(osu_path),
                "Accuracy": acc,
            }
            return result

        except Exception:
            logger.exception(
                "Error calculating PP via rosu-pp for %s", mask_path_for_log(osu_path)
            )
            return None

                    
    def process_osr_with_path(
        self, replay_data, md5_map, not_submitted_cache, osu_api_client=None
    ):
        try:
            if not osu_api_client:
                logger.error("No API client provided in process_osr_with_path")
                return None

            beatmap_md5 = replay_data["beatmap_md5"]
            osr_path = replay_data["osr_path"]

            if beatmap_md5 not in md5_map:
                with self.osr_cache_lock:
                    if beatmap_md5 in not_submitted_cache:
                        logger.debug(
                            "md5 %s already marked as not found, skipping replay: %s",
                            beatmap_md5,
                            mask_path_for_log(osr_path),
                        )
                        return None
                return None

            mtime = os.path.getmtime(osr_path)

                                                                             
            rel_osr_path = self.to_relative_path(osr_path)

            with self.osr_cache_lock:
                                                      
                if (
                    rel_osr_path in self.osr_cache
                    and self.osr_cache[rel_osr_path].get("mtime") == mtime
                ):
                    return self.osr_cache[rel_osr_path].get("result")

                                                                      
                if (
                    osr_path in self.osr_cache
                    and self.osr_cache[osr_path].get("mtime") == mtime
                ):
                    result = self.osr_cache[osr_path].get("result")
                                                                           
                    self.osr_cache[rel_osr_path] = self.osr_cache[osr_path]
                                                       
                    del self.osr_cache[osr_path]
                    return result

            osu_path = md5_map[beatmap_md5]
            res = self.calculate_pp_rosu(osu_path, replay_data)

            if res:
                beatmap_id = res.get("beatmap_id")

                if beatmap_id == 0:
                    return None

                elif beatmap_id is None:
                                                                                                    
                                                                                                        
                    md5 = replay_data.get("beatmap_md5")
                    if md5 is None:
                        return None

                    if md5 in self.md5_beatmapid_cache:
                        res["beatmap_id"] = self.md5_beatmapid_cache[md5]
                    else:
                        try:
                            beatmap_data = osu_api_client.lookup_osu(md5)
                            if isinstance(beatmap_data, dict) and "id" in beatmap_data:
                                                   
                                new_id = beatmap_data.get("id")

                                                                        
                                db_save(
                                    new_id,
                                    beatmap_data.get("status", "unknown"),
                                    beatmap_data.get("artist", ""),
                                    beatmap_data.get("title", ""),
                                    beatmap_data.get("version", ""),
                                    beatmap_data.get("creator", ""),
                                    beatmap_data.get("hit_objects", 0),
                                )

                                                           
                                self.md5_beatmapid_cache[md5] = new_id
                                res["beatmap_id"] = new_id

                                                                           
                                if "artist" not in res or not res["artist"]:
                                    res["artist"] = beatmap_data.get("artist", "")
                                if "title" not in res or not res["title"]:
                                    res["title"] = beatmap_data.get("title", "")
                                if "creator" not in res or not res["creator"]:
                                    res["creator"] = beatmap_data.get("creator", "")
                                if "version" not in res or not res["version"]:
                                    res["version"] = beatmap_data.get("version", "")

                            elif beatmap_data is not None:
                                                                                                     
                                new_id = beatmap_data
                                self.md5_beatmapid_cache[md5] = new_id
                                res["beatmap_id"] = new_id
                        except Exception as e:
                            logger.error(
                                "Error when requesting beatmap_id by md5 (%s): %s",
                                md5,
                                e,
                            )

                if res.get("beatmap_id") is None:
                    logger.warning(
                        f"Failed to get beatmap_id for replay {mask_path_for_log(osr_path)}"
                    )
                    return None

                if "player_name" in replay_data:
                    res["player_name"] = replay_data["player_name"]
                if "score_time" in replay_data:
                    res["score_time"] = replay_data["score_time"]

                with self.osr_cache_lock:
                                                     
                    self.osr_cache[rel_osr_path] = {"mtime": mtime, "result": res}
            return res
        except Exception as e:
            logger.exception(f"Unexpected error processing replay with path: {e}")
            return None

    def download_osu_file(self, beatmap_id, osu_api_client=None):
                   
        try:
            if not beatmap_id:
                logger.error("Cannot download .osu file: beatmap_id is None or 0")
                return None

            if not osu_api_client:                   
                logger.error("No API client provided for downloading .osu file")
                return None

            filename = f"beatmap_{beatmap_id}.osu"
            file_path = os.path.join(MAPS_DIR, filename)

            if os.path.exists(file_path):
                logger.debug(
                    f"Beatmap file already exists: {mask_path_for_log(file_path)}"
                )
                return file_path

                                                                                                         
            url = f"https://osu.ppy.sh/osu/{beatmap_id}"
            logger.info(f"GET beatmap file: {url}")

            @osu_api_client._retry_request
            def download_beatmap():
                osu_api_client._wait_for_api_slot()
                return osu_api_client.session.get(url, timeout=MAP_DOWNLOAD_TIMEOUT)

            logger.debug(f"Downloading .osu file for beatmap_id {beatmap_id}")
            response = download_beatmap()

            if response.status_code == 404:
                logger.warning(
                    f"Beatmap with ID {beatmap_id} not found on server (HTTP 404)"
                )
                return None

            response.raise_for_status()

            file_size = len(response.content)
            logger.debug(f"Download successful: received {file_size} bytes")

            with open(file_path, "wb") as f:
                f.write(response.content)

            logger.debug(f"File saved to {mask_path_for_log(file_path)}")

            cache = self.md5_load()
            self.find_md5(file_path, cache)
            self.md5_save(cache)

            logger.info(
                f"Successfully downloaded and cached .osu file for beatmap_id {beatmap_id}"
            )
            return file_path

        except requests.exceptions.RequestException as e:
            logger.error(
                f"RequestException downloading .osu file for beatmap_id {beatmap_id}: {e}"
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error downloading .osu file for beatmap_id {beatmap_id}: {e}"
            )
            return None

    def update_osu_md5_cache(self, new_osu_path, md5_hash):
        with self.md5_cache_lock:
            cache = {}
            try:
                if os.path.exists(self.md5_cache_path):
                    with open(self.md5_cache_path, "r", encoding="utf-8") as f:
                        cache = json.load(f)
            except Exception as e:
                logger.error(f"Failed to read cache: {e}")

            try:
                mtime = os.path.getmtime(new_osu_path)
            except Exception as e:
                logger.warning(
                    f"Failed to get mtime for {mask_path_for_log(new_osu_path)}: {e}"
                )
                mtime = None

                                                                             
            rel_path = self.to_relative_path(new_osu_path)
            cache[rel_path] = {"mtime": mtime, "md5": md5_hash}

            try:
                with open(self.md5_cache_path, "w", encoding="utf-8") as f:
                    json.dump(cache, f, indent=4)
            except Exception as e:
                logger.error(f"Error updating osu_md5_cache: {e}")

    def count_objs(self, osu_path, beatmap_id, gui_log=None):
        total = 0
        try:
                                           
            with self.file_access_lock:
                if not os.path.exists(osu_path):
                    logger.warning(f"File not found: {mask_path_for_log(osu_path)}")
                    return 0

                if gui_log:
                    gui_log(
                        f"Counting hit objects for beatmap {beatmap_id}",
                        update_last=True,
                    )

                with open(osu_path, "r", encoding="utf-8", errors="ignore") as f:
                    in_hit = False
                    for line in f:
                        line = line.strip()
                        if line.startswith("[HitObjects]"):
                            in_hit = True
                            continue
                        if in_hit and line and not line.startswith("//"):
                            total += 1
        except Exception as e:
            logger.error(
                "Error reading .osu file %s: %s", mask_path_for_log(osu_path), e
            )
            return 0

                                                                        
                                                   
        db_info = db_get(beatmap_id)
        if db_info:
            if not db_info["hit_objects"]:
                db_save(
                    beatmap_id,
                    db_info["status"],
                    db_info["artist"],
                    db_info["title"],
                    db_info["version"],
                    db_info["creator"],
                    total,
                )
        else:
            metadata = self.parse_osu_metadata(osu_path)
            db_save(
                beatmap_id,
                "unknown",
                metadata.get("artist", ""),
                metadata.get("title", ""),
                metadata.get("version", ""),
                metadata.get("creator", ""),
                total,
            )

        return total

    def parse_osu_metadata(self, osu_path):
        result = {"artist": "", "title": "", "creator": "", "version": ""}
        try:
                                           
            with self.file_access_lock:
                if not os.path.exists(osu_path):
                    logger.warning(f"File not found: {mask_path_for_log(osu_path)}")
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
        except Exception as e:
            logger.exception(
                "Error parsing .osu file %s: %s", mask_path_for_log(osu_path), e
            )
        return result

    def grade_osu(self, beatmap_id, c300, c100, c50, cMiss):
        db_info = db_get(beatmap_id)

        total = 0
        if db_info:
            total = db_info.get("hit_objects", 0)
        if not total:
            osu_file = None
            for md5, path in self.md5_map.items():
                if path and os.path.exists(path):
                    bid = self.parse_beatmap_id(path)
                    if bid == beatmap_id:
                        osu_file = path
                        break

            if osu_file:
                total = self.count_objs(osu_file, beatmap_id)
                logger.debug(
                    f"Locally counted {total} objects for beatmap_id {beatmap_id}"
                )

            if not total:
                logger.warning(
                    f"Failed to determine object count for beatmap_id {beatmap_id}"
                )
                return "?"

        c300_corrected = c300
        p300 = (c300_corrected / total) * 100 if total else 0
        p50 = (c50 / total) * 100 if total else 0

        if p300 == 100:
            return "SS"
        elif p300 > 90 and p50 <= 1 and cMiss == 0:
            return "S"
        elif p300 > 90:
            return "A"
        elif p300 > 80 and cMiss == 0:
            return "A"
        elif p300 > 80:
            return "B"
        elif p300 > 70 and cMiss == 0:
            return "B"
        elif p300 > 60:
            return "C"
        else:
            return "D"

    def get_calc_acc(self):
                                                                            
        return self.calc_acc

    def get_sort_mods(self):
                                                                             
        return self.sort_mods

    def get_not_submitted_cache(self):
                                                                                     
        return self.not_submitted_cache
