import hashlib
import json
import concurrent.futures
import struct
import threading
import datetime
import os
import logging
import rosu_pp_py as rosu
import requests
from database import db_get, db_save
from utils import get_resource_path, mask_path_for_log

logger = logging.getLogger(__name__)
cache_folder = get_resource_path("cache")
os.makedirs(cache_folder, exist_ok=True)

                                                                
OSU_BASE_PATH = None

                                     
def set_osu_base_path(path):
    global OSU_BASE_PATH
    if path:
        OSU_BASE_PATH = os.path.normpath(path)
        logger.info(f"osu! base path set to: {mask_path_for_log(OSU_BASE_PATH)}")

                                                             
def to_relative_path(abs_path):
    if not abs_path or not OSU_BASE_PATH:
        return abs_path
    try:
                                                           
        if os.path.normpath(abs_path).startswith(OSU_BASE_PATH):
            rel_path = os.path.relpath(abs_path, OSU_BASE_PATH)
            return rel_path
        return abs_path
    except Exception as e:
        logger.error(f"Error converting to relative path: {e}")
        return abs_path

                                                             
def to_absolute_path(rel_path):
    if not rel_path or not OSU_BASE_PATH:
        return rel_path
    try:
                                                   
        if not os.path.isabs(rel_path):
            abs_path = os.path.normpath(os.path.join(OSU_BASE_PATH, rel_path))
            return abs_path
        return rel_path
    except Exception as e:
        logger.error(f"Error converting to absolute path: {e}")
        return rel_path

MD5_CACHE_LOCK = threading.Lock()
MD5_BEATMAPID_CACHE = {}
MD5_MAP = {}

OSR_CACHE_PATH = os.path.join(get_resource_path("cache"), "osr_cache.json")
MD5_CACHE_PATH = os.path.join(get_resource_path("cache"), "osu_md5_cache.json")

OSR_CACHE = {}
OSR_CACHE_LOCK = threading.Lock()

MAPS_DIR = get_resource_path("maps")
os.makedirs(MAPS_DIR, exist_ok=True)


def reset_in_memory_caches():
    global MD5_BEATMAPID_CACHE, MD5_MAP, OSR_CACHE, NOT_SUBMITTED_CACHE
    with MD5_CACHE_LOCK:
        MD5_BEATMAPID_CACHE.clear()
        MD5_MAP.clear()
    with OSR_CACHE_LOCK:
        OSR_CACHE.clear()
    NOT_SUBMITTED_CACHE.clear()

    logger.info("In-memory caches (MD5, OSR, NotSubmitted) have been reset.")


def not_submitted_cache_load():
    if os.path.exists(NOT_SUBMITTED_CACHE_PATH):
        try:
            with open(NOT_SUBMITTED_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Error reading not_submitted_cache: %s", e)
            return {}
    return {}


def not_submitted_cache_save(cache):
    try:
        with open(NOT_SUBMITTED_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=4)
    except Exception as e:
        logger.error("Error writing not_submitted_cache: %s", e)


NOT_SUBMITTED_CACHE_PATH = os.path.join(get_resource_path("cache"), "not_submitted_cache.json")
NOT_SUBMITTED_CACHE = not_submitted_cache_load()


def osr_load():
    with OSR_CACHE_LOCK:
        if os.path.exists(OSR_CACHE_PATH):
            try:
                with open(OSR_CACHE_PATH, "r", encoding="utf-8") as f:
                    content = f.read()
                    logger.debug("OSR-cache (%s): %s", mask_path_for_log(OSR_CACHE_PATH), content[:200])
                    f.seek(0)
                    return json.load(f)
            except Exception:
                logger.exception("Error reading OSR-cache: %s", mask_path_for_log(OSR_CACHE_PATH))
        return {}

def osr_save(cache):
    with OSR_CACHE_LOCK:
        try:
            with open(OSR_CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=4)
        except Exception as e:
            logger.error("Error saving OSR-cache: %s", e)


def md5_load():
    with MD5_CACHE_LOCK:
        if os.path.exists(MD5_CACHE_PATH):
            try:
                with open(MD5_CACHE_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning("Error reading MD5-cache: %s", e)
                return {}
        return {}

def md5_save(cache):
    with MD5_CACHE_LOCK:
        try:
            with open(MD5_CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=4)
        except Exception as e:
            logger.error("Error saving MD5-cache: %s", e)


def get_md5(path):
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()


def find_md5(full_path, cache):
    try:
        mtime = os.path.getmtime(full_path)
    except Exception:
        return None

                                                                     
    rel_path = to_relative_path(full_path)

                                                            
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

    md5_hash = get_md5(full_path)
    cache[rel_path] = {"mtime": mtime, "md5": md5_hash}
    return md5_hash


OSR_CACHE = osr_load()


def find_osu(songs_folder, progress_callback=None):
    files = []
    for root, dirs, filenames in os.walk(songs_folder):
        for file in filenames:
            if file.endswith(".osu"):
                files.append(os.path.join(root, file))
    total = len(files)
    md5_map = {}
    cache = md5_load()

    def process_file(p):
        val = find_md5(p, cache)
        return (val, p)

    count = 0
    with concurrent.futures.ThreadPoolExecutor() as ex:
        futs = {ex.submit(process_file, p): p for p in files}
        for fut in concurrent.futures.as_completed(futs):
            res = fut.result()
            if res and res[0]:
                md5_map[res[0]] = res[1]
            count += 1
            if progress_callback:
                progress_callback(count, total)
    md5_save(cache)

    global MD5_MAP
    MD5_MAP = md5_map

    return md5_map


def read_string(data, offset):
    if data[offset] == 0x00:
        return "", offset + 1
    elif data[offset] == 0x0b:
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
        s = data[offset:offset + length].decode('utf-8', errors='ignore')
        return s, offset + length
    else:
        raise ValueError("Invalid string in .osr")


MODS_MAPPING_ITER = [
    (1, "NF"), (2, "EZ"), (8, "HD"), (16, "HR"), (32, "SD"),
    (64, "DT"), (128, "RX"), (256, "HT"), (512, "NC"), (1024, "FL"),
    (4096, "SO"), (8192, "AP"), (536870912, "SCOREV2")
]
DISALLOWED_MODS = {"RX", "AT", "AP", "SCOREV2"}


def parse_osu_metadata(osu_path):
    result = {
        "artist": "",
        "title": "",
        "creator": "",
        "version": ""
    }
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
        logger.exception("Error parsing .osu file %s: %s", mask_path_for_log(osu_path), e)
    return result


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
    except Exception:
        pass

    return beatmap_id


def parse_mods(mods_int):
    mods = []
    if mods_int & 512:
        mods.append("NC")
    if mods_int & 16384:
        mods.append("PF")
    for bit, name in MODS_MAPPING_ITER:
        if mods_int & bit:
            mods.append(name.upper())
    return tuple(sorted(set(mods), key=lambda x: x))


def sort_mods(mod_list):
    priority = {"EZ": 1, "HD": 2, "DT": 3, "NC": 3, "HT": 3,
                "HR": 4, "FL": 5, "NF": 6, "SO": 7}
    out = [m for m in mod_list if m != "CL"]
    out.sort(key=lambda m: (priority.get(m, 9999), m))
    return out


def parse_osr(osr_path):
    with open(osr_path, "rb") as f:
        data = f.read()
    offset = 0
    mode = data[offset]
    offset += 1

    offset += 4
    beatmap_md5, offset = read_string(data, offset)
    player, offset = read_string(data, offset)
    _, offset = read_string(data, offset)

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
    full_combo = (perfect == 0x01)
    mods_int = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    mods = parse_mods(mods_int)
    if any(m in DISALLOWED_MODS for m in mods):
        return None

    _, offset = read_string(data, offset)
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
        "score_time": tstr
    }


def calc_acc(c300, c100, c50, cmiss):
    hits = c300 + c100 + c50 + cmiss
    if hits == 0:
        return 100.0
    return round((300 * c300 + 100 * c100 + 50 * c50) / (300 * hits) * 100, 2)


def calculate_pp_rosu(osu_path, replay):
    try:
        beatmap = rosu.Beatmap(path=osu_path)
        acc = calc_acc(
            replay["count300"],
            replay["count100"],
            replay["count50"],
            replay["countMiss"]
        )

        original_mods = replay["mods_list"]

        mods_for_perf = list(original_mods)
        if "CL" not in mods_for_perf:
            mods_for_perf.append("CL")

        priority = {"EZ": 1, "HD": 2, "DT": 3, "NC": 3, "HT": 3, "HR": 4, "FL": 5, "NF": 6, "SO": 7}
        sorted_mods_perf = sorted(mods_for_perf, key=lambda m: (priority.get(m, 9999), m))
        mods_string = "".join(sorted_mods_perf)

        perf = rosu.Performance(
            accuracy=acc,
            combo=replay["max_combo"],
            misses=replay["countMiss"],
            mods=mods_string
        )
        attrs = perf.calculate(beatmap)
        if not attrs:
            return None

        bm_id = parse_beatmap_id(osu_path)
        meta = parse_osu_metadata(osu_path)

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
            "osu_file_path": osu_path,
            "Accuracy": acc,
        }
        return result

    except Exception as e:
        logger.exception("Error calculating PP via rosu-pp for %s", mask_path_for_log(osu_path))
        return None


def proc_osr(osr_path, md5_map, cutoff, username):
    try:
        rep = parse_osr(osr_path)
        if not rep:
            logger.warning("Failed to process osr: %s", mask_path_for_log(osr_path))
            return None
        if rep["game_mode"] != 0:
            return None
        if rep["player_name"].lower() != username.lower():
            return None
        if rep["beatmap_md5"] not in md5_map:
            with OSR_CACHE_LOCK:
                if rep["beatmap_md5"] in NOT_SUBMITTED_CACHE:
                    logger.error("md5 %s already marked as not found, skipping replay: %s", rep["beatmap_md5"],
                                 mask_path_for_log(osr_path))
                    return None

            from osu_api import lookup_osu
            beatmap_id_api = lookup_osu(rep["beatmap_md5"])
            if beatmap_id_api and beatmap_id_api != 0:
                new_osu_path = download_osu_file(beatmap_id_api)
                if new_osu_path:
                    md5_map[rep["beatmap_md5"]] = new_osu_path
                    update_osu_md5_cache(new_osu_path, rep["beatmap_md5"])
                    logger.info("Downloaded new .osu file for beatmap_id %s by md5 %s", beatmap_id_api,
                                rep["beatmap_md5"])
                else:
                    logger.error("Failed to download .osu file for beatmap_id %s", beatmap_id_api)

                    with OSR_CACHE_LOCK:
                        NOT_SUBMITTED_CACHE[rep["beatmap_md5"]] = True
                        not_submitted_cache_save(NOT_SUBMITTED_CACHE)
                    return None
            else:
                logger.error("No .osu file for replay: %s with md5: %s", mask_path_for_log(osr_path),
                             rep["beatmap_md5"])

                with OSR_CACHE_LOCK:
                    NOT_SUBMITTED_CACHE[rep["beatmap_md5"]] = True
                    not_submitted_cache_save(NOT_SUBMITTED_CACHE)
                return None

        mtime = os.path.getmtime(osr_path)

                                                                         
        rel_osr_path = to_relative_path(osr_path)

        with OSR_CACHE_LOCK:
                                                  
            if rel_osr_path in OSR_CACHE and OSR_CACHE[rel_osr_path].get("mtime") == mtime:
                return OSR_CACHE[rel_osr_path].get("result")

                                                                  
            if osr_path in OSR_CACHE and OSR_CACHE[osr_path].get("mtime") == mtime:
                result = OSR_CACHE[osr_path].get("result")
                                                                       
                OSR_CACHE[rel_osr_path] = OSR_CACHE[osr_path]
                                                   
                del OSR_CACHE[osr_path]
                return result

        osu_path = md5_map[rep["beatmap_md5"]]
        res = calculate_pp_rosu(osu_path, rep)
        if res:
            beatmap_id = res.get("beatmap_id")

            if beatmap_id == 0:
                return None

            elif beatmap_id is None:
                md5 = rep.get("beatmap_md5")
                if md5 is None:
                    return None

                if md5 in MD5_BEATMAPID_CACHE:
                    res["beatmap_id"] = MD5_BEATMAPID_CACHE[md5]
                else:
                    try:
                        from osu_api import lookup_osu
                        new_id = lookup_osu(md5)
                        if new_id is not None:
                            MD5_BEATMAPID_CACHE[md5] = new_id
                            res["beatmap_id"] = new_id
                    except Exception as e:
                        logger.error("Error when requesting beatmap_id by md5 (%s): %s", md5, e)

            if res.get("beatmap_id") is None:
                logger.warning(f"Failed to get beatmap_id for replay {mask_path_for_log(osr_path)}")
                return None

            if isinstance(rep, dict):
                if "player_name" in rep:
                    res["player_name"] = rep["player_name"]
                if "score_time" in rep:
                    res["score_time"] = rep["score_time"]

                with OSR_CACHE_LOCK:
                                                     
                    OSR_CACHE[rel_osr_path] = {"mtime": mtime, "result": res}
            else:
                logger.warning(f"Invalid replay format for {mask_path_for_log(osr_path)}: {type(rep)}")
                return None
        return res
    except Exception as e:
        logger.exception(f"Unexpected error processing replay {mask_path_for_log(osr_path)}: {e}")
        return None


def download_osu_file(beatmap_id):
    filename = f"beatmap_{beatmap_id}.osu"
    file_path = os.path.join(MAPS_DIR, filename)

    if os.path.exists(file_path):
        return file_path

    url = f"https://osu.ppy.sh/osu/{beatmap_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        logger.error("Error downloading .osu file for beatmap_id %s: %s", beatmap_id, e)
        return None

    with open(file_path, "wb") as f:
        f.write(response.content)

    from file_parser import md5_load, find_md5, md5_save

    cache = md5_load()

    find_md5(file_path, cache)

    md5_save(cache)

    return file_path


def update_osu_md5_cache(new_osu_path, md5_hash):
    global MD5_CACHE_PATH
    with MD5_CACHE_LOCK:
        cache = {}
        try:
            if os.path.exists(MD5_CACHE_PATH):
                with open(MD5_CACHE_PATH, "r", encoding="utf-8") as f:
                    cache = json.load(f)
        except Exception as e:
            logger.error(f"Failed to read cache: {e}")

        try:
            mtime = os.path.getmtime(new_osu_path)
        except Exception as e:
            logger.warning(f"Failed to get mtime for {mask_path_for_log(new_osu_path)}: {e}")
            mtime = None

                                                                         
        rel_path = to_relative_path(new_osu_path)
        cache[rel_path] = {"mtime": mtime, "md5": md5_hash}

        try:
            with open(MD5_CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=4)
        except Exception as e:
            logger.error(f"Error updating osu_md5_cache: {e}")

def count_objs(osu_path, beatmap_id):
    total = 0
    try:
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
        logger.error("Error reading .osu file %s: %s", mask_path_for_log(osu_path), e)
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
                total
            )
    else:

        metadata = parse_osu_metadata(osu_path)
        db_save(
            beatmap_id,
            "unknown",
            metadata.get("artist", ""),
            metadata.get("title", ""),
            metadata.get("version", ""),
            metadata.get("creator", ""),
            total
        )

    return total


def grade_osu(beatmap_id, c300, c100, c50, cMiss):
    from database import db_get
    db_info = db_get(beatmap_id)

    total = 0
    if db_info:
        total = db_info.get("hit_objects", 0)

    if not total:

        osu_file = None
        for md5, path in MD5_MAP.items():
            if path and os.path.exists(path):
                bid = parse_beatmap_id(path)
                if bid == beatmap_id:
                    osu_file = path
                    break

        if osu_file:
            total = count_objs(osu_file, beatmap_id)
            logger.info(f"Locally counted {total} objects for beatmap_id {beatmap_id}")

        if not total:
            logger.warning(f"Failed to determine object count for beatmap_id {beatmap_id}")
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
