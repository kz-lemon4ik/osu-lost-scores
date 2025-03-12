import hashlib
import subprocess
import json
import concurrent.futures
import struct
import threading
import datetime
import os
import logging
from config import PERFORMANCE_CALCULATOR_PATH

logger = logging.getLogger(__name__)

OSR_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", "cache", "osr_cache.json")
MD5_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", "cache", "osu_md5_cache.json")

OSR_CACHE = {}
OSR_CACHE_LOCK = threading.Lock()

def osr_load():
                                     
    if os.path.exists(OSR_CACHE_PATH):
        try:
            with open(OSR_CACHE_PATH, "r", encoding="utf-8") as f:
                content = f.read()
                logger.debug("OSR-кэш (%s): %s", OSR_CACHE_PATH, content[:200])
                f.seek(0)
                return json.load(f)
        except Exception as e:
            logger.exception("Ошибка чтения OSR-кэша: %s", OSR_CACHE_PATH)
    return {}

def osr_save(cache):
                                   
    with open(OSR_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=4)

def md5_load():
                                               
    if os.path.exists(MD5_CACHE_PATH):
        try:
            with open(MD5_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Ошибка чтения MD5-кэша: %s", e)
            return {}
    return {}

def md5_save(cache):
                            
    with open(MD5_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=4)

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
    if full_path in cache:
        saved = cache[full_path]
        if saved.get("mtime") == mtime:
            return saved.get("md5")
    md5_hash = get_md5(full_path)
    cache[full_path] = {"mtime": mtime, "md5": md5_hash}
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
    return md5_map

def read_string(data, offset):
                                       
    if data[offset] == 0x00:
        return "", offset+1
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
        s = data[offset:offset+length].decode('utf-8', errors='ignore')
        return s, offset + length
    else:
        raise ValueError("Неправильная строка в .osr")

MODS_MAPPING_ITER = [
    (1, "NF"), (2, "EZ"), (8, "HD"), (16, "HR"), (32, "SD"),
    (64, "DT"), (128, "RX"), (256, "HT"), (512, "NC"), (1024, "FL"),
    (4096, "SO"), (8192, "AP"), (536870912, "SCOREV2")
]
DISALLOWED_MODS = {"RX", "AT", "AP", "SCOREV2"}

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
    return round((300*c300 + 100*c100 + 50*c50) / (300*hits) * 100, 2)

external_process_semaphore = threading.Semaphore(1)

def proc_osr(osr_path, md5_map, cutoff, username):
                                                       
    rep = parse_osr(osr_path)
    if not rep:
        logger.warning("Не удалось обработать osr: %s", osr_path)
        return None
    if rep["score_timestamp"] >= cutoff or rep["game_mode"] != 0:
        return None
    if rep["player_name"].lower() != username.lower():
        return None
    if rep["beatmap_md5"] not in md5_map:
        logger.error("Нет .osu для %s с md5: %s", osr_path, rep["beatmap_md5"])
        return None

    mtime = os.path.getmtime(osr_path)
    with OSR_CACHE_LOCK:
        if osr_path in OSR_CACHE and OSR_CACHE[osr_path].get("mtime") == mtime:
            return OSR_CACHE[osr_path].get("result")

    osu_path = md5_map[rep["beatmap_md5"]]
    res = calculate_pp_local(osu_path, rep)
    if res:
        res["player_name"] = rep["player_name"]
        res["score_time"] = rep["score_time"]
        with OSR_CACHE_LOCK:
            OSR_CACHE[osr_path] = {"mtime": mtime, "result": res}
    return res

def calculate_pp_local(osu_path, replay):
                                              
    acc = calc_acc(replay["count300"], replay["count100"], replay["count50"], replay["countMiss"])
    acc_str = f"{acc:.2f}".replace('.', ',')
    mods = list(replay["mods_list"])
    if "CL" not in mods:
        mods.append("CL")
    cmd = [
        PERFORMANCE_CALCULATOR_PATH,
        "simulate", "osu", osu_path,
        "-c", str(replay["max_combo"]),
        "-a", acc_str,
        "-X", str(replay["countMiss"]),
        "-j"
    ]
    for m in mods:
        cmd += ["-m", m]

    try:
        with external_process_semaphore:
            r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", check=True)
        out = r.stdout.strip()
        if not out:
            logger.warning("Пустой вывод PP для %s. Команда: %s", osu_path, ' '.join(cmd))
            return None
    except subprocess.CalledProcessError as e:
        logger.error("Ошибка PP-калькулятора для %s. Код: %s. Вывод: %s.", osu_path, e.returncode, e.output)
        return None
    except Exception as e:
        logger.exception("Непредвиденная ошибка PP-калькулятора для %s", osu_path)
        return None

    try:
        data = json.loads(out)
        score_info = data.get("score", {})
        pp_val = data.get("performance_attributes", {}).get("pp")
        if pp_val is None:
            logger.error("PP не найден для %s. Данные: %s", osu_path, data)
            return None
        return {
            "pp": round(float(pp_val)),
            "beatmap_id": score_info.get("beatmap_id"),
            "Beatmap": score_info.get("beatmap"),
            "total_score": replay["total_score"],
            "mods": tuple(sorted(set(mods))),
            "count100": replay["count100"],
            "count50": replay["count50"],
            "countMiss": replay["countMiss"],
            "count300": replay["count300"],
            "osu_file_path": osu_path,
            "Accuracy": acc
        }
    except Exception as e:
        logger.exception("Ошибка JSON PP-калькулятора для %s. Вывод: %s", osu_path, out)
        return None

def count_objs(osu_path):
                                                               
    total = 0
    with open(osu_path, "r", encoding="utf-8") as f:
        in_hit = False
        for line in f:
            line = line.strip()
            if line.startswith("[HitObjects]"):
                in_hit = True
                continue
            if in_hit and line and not line.startswith("//"):
                total += 1
    return total

def grade_osu(osu_path, c300, c50, cmiss):
                                                
    t = count_objs(osu_path)
    if t == 0:
        return "D"
    p300 = (c300 / t) * 100
    p50 = (c50 / t) * 100
    if p300 == 100:
        return "SS"
    elif p300 > 90 and p50 <= 1 and cmiss == 0:
        return "S"
    elif p300 > 90:
        return "A"
    elif p300 > 80 and cmiss == 0:
        return "A"
    elif p300 > 80:
        return "B"
    elif p300 > 70 and cmiss == 0:
        return "B"
    elif p300 > 60:
        return "C"
    else:
        return "D"
