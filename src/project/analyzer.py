import os
import json
import csv
import calendar
import time
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from file_parser import parse_osr, grade_osu

from database import db_init, db_get, db_save

from osu_api import token_osu, user_osu, top_osu, map_osu

from file_parser import find_osu, proc_osr, calc_acc, sort_mods
from config import CUTOFF_DATE

logger = logging.getLogger(__name__)


def find_lost_scores(scores):
    if not scores:
        logger.warning("Пустой список скоров в find_lost_scores")
        return []

                                      
    valid_scores = []
    for rec in scores:
        try:
                              
            if not isinstance(rec, dict):
                logger.warning(f"Скор не является словарем: {type(rec)}")
                continue

                                                
            if "beatmap_id" not in rec or rec["beatmap_id"] is None:
                logger.warning(f"Скор не содержит beatmap_id или он равен None")
                continue

            if not all(key in rec for key in ["mods", "pp", "total_score"]):
                logger.warning(f"Скор не содержит все необходимые ключи: {rec.keys()}")
                continue

                                                                                   
            try:
                rec["pp_float"] = float(rec["pp"])
            except (ValueError, TypeError):
                logger.warning(f"Не удалось преобразовать PP в число: {rec.get('pp')}")
                rec["pp_float"] = 0.0

            try:
                rec["total_int"] = int(rec["total_score"])
            except (ValueError, TypeError):
                logger.warning(f"Не удалось преобразовать total_score в число: {rec.get('total_score')}")
                rec["total_int"] = 0

            valid_scores.append(rec)
        except Exception as e:
            logger.warning(f"Ошибка при проверке скора: {e}")
            continue

    if not valid_scores:
        logger.warning("Нет валидных скоров для анализа")
        return []

    groups = {}
    for rec in valid_scores:
        try:
            key = (rec["beatmap_id"], tuple(rec["mods"]))
            groups.setdefault(key, []).append(rec)
        except Exception as e:
            logger.warning(f"Ошибка при группировке скора: {e}")
            continue

    possible_lost = {}
    for key, recs in groups.items():
        try:
            if len(recs) < 2:
                continue

            best_pp = max(recs, key=lambda s: s["pp_float"])
            best_total = max(recs, key=lambda s: s["total_int"])

            if not all(k in best_pp for k in ["total_score", "pp", "beatmap_id"]):
                logger.warning(f"best_pp не содержит необходимые ключи")
                continue

            if not all(k in best_total for k in ["total_score", "pp"]):
                logger.warning(f"best_total не содержит необходимые ключи")
                continue

            pp_better = best_pp["pp_float"] > best_total["pp_float"]
            score_worse = best_pp["total_int"] < best_total["total_int"]

            if score_worse and pp_better:
                bid = best_pp["beatmap_id"]
                possible_lost.setdefault(bid, []).append(best_pp)
        except Exception as e:
            logger.warning(f"Ошибка при обработке группы скоров: {e}")
            continue

    lost_results = []
    map_scores = {}
    for rec in valid_scores:
        map_scores.setdefault(rec["beatmap_id"], []).append(rec)

    for bid, candidates in possible_lost.items():
        try:
            if not candidates:
                continue

            candidate = max(candidates, key=lambda s: s["pp_float"])
            all_scores = map_scores.get(bid, [])

            if not all_scores:
                continue

            best_score = max(all_scores, key=lambda s: s["pp_float"])

            if candidate["pp_float"] >= best_score["pp_float"]:
                lost_results.append(candidate)
        except Exception as e:
            logger.warning(f"Ошибка при обработке потенциально потерянного скора: {e}")
            continue

    try:
        lost_results.sort(key=lambda s: s["pp_float"], reverse=True)
    except Exception as e:
        logger.warning(f"Ошибка при сортировке результатов: {e}")

    return lost_results

def parse_top(raw, token):
    def format_date(iso_str):
        if not iso_str:
            return ""
        try:
            dt = datetime.datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
            return dt.strftime("%d-%m-%Y %H-%M-%S")
        except Exception:
            return iso_str

    parsed = []
    for score in raw:
        try:
            s_id = score.get("id", "")
            created_raw = score.get("created_at", "")
            created = format_date(created_raw)
            pp_val = round(float(score.get("pp", 0)))
            total = score.get("score", 0)
            mods = score.get("mods", [])
            stats = score.get("statistics", {})
            c100 = stats.get("count_100", 0)
            c50 = stats.get("count_50", 0)
            cmiss = stats.get("count_miss", 0)
            c300 = stats.get("count_300", 0)
            acc = calc_acc(c300, c100, c50, cmiss)

            beatmap = score.get("beatmap", {})
            beatmapset = score.get("beatmapset", {})
            bid = beatmap.get("id")
            if bid is None:
                continue

            artist = beatmapset.get("artist", "")
            title = beatmapset.get("title", "")
            creator = beatmapset.get("creator", "")
            version = beatmap.get("version", "")
            full_name = f"{artist} - {title} ({creator}) [{version}]"

            status = beatmap.get("status", "unknown")
            rank = score.get("rank", "")
            parsed.append({
                "Score ID": s_id,
                "PP": pp_val,
                "Beatmap ID": bid,
                "Beatmap": full_name,
                "Mods": ", ".join(mods) if mods else "NM",
                "Score": total,
                "100": c100,
                "50": c50,
                "Misses": cmiss,
                "Status": status,
                "Accuracy": acc,
                "Score Date": created,
                "total_score": total,
                "Rank": rank
            })
        except Exception as e:
            logger.exception("Ошибка топ-результата: %s", e)
            continue
    return parsed


def calc_weight(data):
    ranked = sorted(data, key=lambda x: x["PP"], reverse=True)
    for i, entry in enumerate(ranked):
        mult = 0.95 ** i
        entry["weight_%"] = round(mult * 100, 2)
        entry["weight_PP"] = round(entry["PP"] * mult, 2)
    return ranked


def save_csv(filename, data, extra=None, fields=None):
    if not data:
        return
    cols = fields if fields else list(data[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for row in data:
            writer.writerow({k: row.get(k, "") for k in cols})
        if extra:
            for row in extra:
                writer.writerow({k: row.get(k, "") for k in cols})


def scan_replays(game_dir, user_identifier, lookup_key, progress_callback=None, gui_log=None, include_unranked=False):
                      
    if progress_callback:
        progress_callback(0, 100)                      
    if gui_log:
        gui_log("Инициализация...", update_last=True)

                                           
    if not os.path.isdir(game_dir):
        error_msg = f"Директория игры не существует: {game_dir}"
        logger.error(error_msg)
        if gui_log:
            gui_log(error_msg, False)
        raise ValueError(error_msg)

    songs = os.path.join(game_dir, "Songs")
    replays = os.path.join(game_dir, "Data", "r")

    if not os.path.isdir(songs):
        error_msg = f"Директория Songs не найдена: {songs}"
        logger.error(error_msg)
        if gui_log:
            gui_log(error_msg, False)
        raise ValueError(error_msg)

    if not os.path.isdir(replays):
        error_msg = f"Директория реплеев не найдена: {replays}"
        logger.error(error_msg)
        if gui_log:
            gui_log(error_msg, False)
        raise ValueError(error_msg)

    try:
        db_init()
    except Exception as e:
        error_msg = f"Ошибка инициализации БД: {e}"
        logger.error(error_msg)
        if gui_log:
            gui_log(error_msg, False)
        raise

    try:
        token = token_osu()
        if not token:
            error_msg = "Не удалось получить токен API"
            logger.error(error_msg)
            if gui_log:
                gui_log(error_msg, False)
            raise ValueError(error_msg)

        user_json = user_osu(user_identifier, lookup_key, token)
        if not user_json:
            error_msg = f"Ошибка: Не удалось получить данные пользователя '{user_identifier}' (тип: {lookup_key})."
            logger.error(error_msg)
            if gui_log:
                gui_log(error_msg, False)
            raise ValueError(f"Пользователь не найден: {user_identifier}")

        username = user_json["username"]
        user_id = user_json["id"]

        profile_link = f"https://osu.ppy.sh/users/{user_id}"
        logger.info(f"Найден пользователь: {username} (ID: {user_id})")
        if gui_log:
            gui_log(f"Найден пользователь: {username} ({profile_link})", False)
    except Exception as e:
        error_msg = f"Ошибка при получении данных пользователя: {e}"
        logger.error(error_msg)
        if gui_log:
            gui_log(error_msg, False)
        raise

    gui_log("Сканирую .osu файлы в Songs: 0%", update_last=True)
    last_songs_update = {"time": 0}

    def update_songs(curr, tot):
        now = time.time()
        if now - last_songs_update["time"] >= 1 or curr == tot:
            last_songs_update["time"] = now
            pct = int((curr / tot) * 100)
            if gui_log:
                gui_log(f"Сканирую .osu файлы в Songs: {pct}%", update_last=True)
                                                    
            if progress_callback:
                progress_callback(int(pct * 0.2), 100)

    md5_map = find_osu(songs, progress_callback=update_songs)

                                                        
    if progress_callback:
        progress_callback(20, 100)                 

    gui_log("Сканирование .osu файлов в Songs: 100%", update_last=True)
    gui_log(f"Найдено {len(md5_map)} osu файлов в Songs.", update_last=False)
    cutoff = CUTOFF_DATE

    rep_files = [f for f in os.listdir(replays) if f.endswith(".osr")]
    total_rep = len(rep_files)
    gui_log(f"Обработано 0/{total_rep} реплеев", update_last=True)

    start = time.time()
    score_list = []
    count = 0
    last_replay_update = {"time": 0}

                                                            
    def update_replays(curr, tot):
                                
        if progress_callback:
            progress_callback(20 + int((curr / tot) * 60), 100)

    no_beatmap_id = []
    no_osu_file = []

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {
            executor.submit(
                proc_osr, os.path.join(replays, f), md5_map, cutoff, username
            ): f for f in rep_files
        }
        for fut in as_completed(futures):
            count += 1
            update_replays(count, total_rep)                                             
            osr_filename = futures[fut]
            res = fut.result()
            if res is not None:
                score_list.append(res)
            else:

                try:
                    rep = parse_osr(os.path.join(replays, osr_filename))
                    if rep:

                        if rep["beatmap_md5"] not in md5_map:
                            no_osu_file.append({
                                "PP": "",
                                "Beatmap ID": None,
                                "Beatmap": "",
                                "Mods": "",
                                "100": "",
                                "50": "",
                                "Misses": "",
                                "Accuracy": "",
                                "Score": "",
                                "Date": rep.get("score_time", "")
                            })
                        else:

                            no_beatmap_id.append({
                                "PP": "",
                                "Beatmap ID": None,
                                "Beatmap": "",
                                "Mods": "",
                                "100": "",
                                "50": "",
                                "Misses": "",
                                "Accuracy": "",
                                "Score": "",
                                "Date": rep.get("score_time", "")
                            })
                    else:
                        logger.warning("Невозможно распарсить реплей: %s", osr_filename)
                except Exception as e:
                    logger.exception("Ошибка при обработке проблемного реплея %s: %s", osr_filename, e)

            now = time.time()
            if now - last_replay_update["time"] >= 1 or count == total_rep:
                last_replay_update["time"] = now
                gui_log(f"Обработано {count}/{total_rep} реплеев", update_last=True)

    from file_parser import OSR_CACHE, osr_save
    osr_save(OSR_CACHE)

    elapsed = time.time() - start
    gui_log(f"Сканирование реплеев завершено за {elapsed:.2f} сек. Найдено {len(score_list)} результатов.",
            update_last=False)

                                     
    if gui_log:
        gui_log("Обрабатываю потерянные скоры...", update_last=False)
    if progress_callback:
        progress_callback(80, 100)

    lost = find_lost_scores(score_list)
    lost = [r for r in lost if calendar.timegm(time.strptime(r["score_time"], "%d-%m-%Y %H-%M-%S")) < cutoff]
    logger.info("Найдено %d потерянных скоров (до cutoff)", len(lost))

                                              
    logger.info(f"Include unranked/loved beatmaps: {include_unranked}")

                                                             
    if include_unranked:
        logger.info(f"ВКЛЮЧЕНЫ unranked/loved карты. Получаем информацию локально. Всего скоров: {len(lost)}")

                                            
        for i, rec in enumerate(lost):
            beatmap_id = rec["beatmap_id"]

                                                          
            osu_file_path = rec.get("osu_file_path")
            if osu_file_path and os.path.exists(osu_file_path):
                                                                     
                from file_parser import count_objs, parse_osu_metadata
                hit_objects = count_objs(osu_file_path, beatmap_id)
                metadata = parse_osu_metadata(osu_file_path)

                                                                        
                db_save(
                    beatmap_id,
                    "unknown",                                   
                    metadata.get("artist", rec.get("artist", "")),
                    metadata.get("title", rec.get("title", "")),
                    metadata.get("version", rec.get("version", "")),
                    metadata.get("creator", rec.get("creator", "")),
                    hit_objects
                )
            else:
                logger.warning(f"Не найден локальный .osu файл для скора с beatmap_id {beatmap_id}")

                                                                               
            rec["Status"] = "unknown"

            if gui_log:
                gui_log(f"Обработка карты {beatmap_id} ({i + 1}/{len(lost)})", update_last=True)
            if progress_callback:
                progress_callback(80 + int((i / len(lost)) * 15), 100)

                                          
        logger.info(f"ВКЛЮЧЕНЫ unranked/loved карты. Всего скоров: {len(lost)}")

    else:
                                                                          
        logger.info(f"Проверяем статус для {len(lost)} карт...")

                                                   
        for i, rec in enumerate(lost):
            db_ = db_get(rec["beatmap_id"])

                                                             
                                    
                                                                
            need_api_update = False
            if not db_:
                need_api_update = True
            elif not include_unranked and db_.get("status") == "unknown":
                need_api_update = True

            if need_api_update:
                from osu_api import map_osu
                info_api = map_osu(rec["beatmap_id"], token)
                if gui_log:
                    gui_log(f"Получение информации о карте {rec['beatmap_id']} ({i + 1}/{len(lost)})", update_last=True)
                if progress_callback:
                    progress_callback(80 + int((i / len(lost)) * 15), 100)

                if info_api:
                    db_save(
                        rec["beatmap_id"],
                        info_api["status"],
                        info_api["artist"],
                        info_api["title"],
                        info_api["version"],
                        info_api["creator"],
                        info_api.get("hit_objects", 0)
                    )
                    db_ = info_api
                else:
                    db_ = {
                        "status": "unknown",
                        "artist": rec.get("artist", ""),
                        "title": rec.get("title", ""),
                        "version": rec.get("version", ""),
                        "creator": rec.get("creator", ""),
                        "hit_objects": 0
                    }
                    db_save(
                        rec["beatmap_id"],
                        db_["status"],
                        db_["artist"],
                        db_["title"],
                        db_["version"],
                        db_["creator"],
                        0
                    )

            rec["Status"] = db_.get("status", "unknown")

                                                
        original_count = len(lost)
        lost = [r for r in lost if r.get("Status") in ["ranked", "approved"]]
        filtered_count = len(lost)
        logger.info(f"Отфильтровано {original_count - filtered_count} скоров, осталось: {filtered_count}")

    for rec in lost:
        db_info = db_get(rec["beatmap_id"])
        if not db_info or not db_info.get("hit_objects", 0):
            pass

    if gui_log:
        gui_log("Сохранение результатов...", update_last=True)
    if progress_callback:
        progress_callback(95, 100)

    if lost:
        out_file = os.path.join(os.path.dirname(__file__), "..", "csv", "lost_scores.csv")
        fields = ["PP", "Beatmap ID", "Beatmap", "Mods", "100", "50", "Misses",
                  "Accuracy", "Score", "Date", "Rank"]

        csv_dir = os.path.dirname(out_file)
        os.makedirs(csv_dir, exist_ok=True)

        while True:
            try:
                with open(out_file, "w", newline="", encoding="utf-8") as csvf:
                    writer = csv.DictWriter(csvf, fieldnames=fields)
                    writer.writeheader()
                    for rec in lost:
                        c100 = rec.get("count100", 0)
                        c50 = rec.get("count50", 0)
                        cMiss = rec.get("countMiss", 0)
                        c300 = rec.get("count300", 0)

                        rank_ = grade_osu(rec["beatmap_id"], c300, c100, c50, cMiss)

                        writer.writerow({
                            "PP": rec["pp"],
                            "Beatmap ID": rec["beatmap_id"],
                            "Beatmap": f"{rec.get('artist', '')} - {rec.get('title', '')} ({rec.get('creator', '')}) [{rec.get('version', '')}]",
                            "Mods": ", ".join(sort_mods(rec["mods"])) if rec["mods"] else "NM",
                            "100": c100,
                            "50": c50,
                            "Misses": cMiss,
                            "Accuracy": rec["Accuracy"],
                            "Score": rec.get("total_score", ""),
                            "Date": rec.get("score_time", ""),
                            "Rank": rank_
                        })
                gui_log(f"Файл lost_scores.csv сохранен.", update_last=True)
                break
            except PermissionError:
                logger.warning("Файл %s занят, повторяю через 0.5 сек.", out_file)
                time.sleep(0.5)
            except Exception as e:
                logger.exception("Ошибка записи %s: %s", out_file, e)
                break
    else:
        logger.info("Пусто: потерянные скоры не записаны.")

                     
    if progress_callback:
        progress_callback(100, 100)                  


def make_top(game_dir, user_identifier, lookup_key, gui_log=None, progress_callback=None):
                                                              
    if progress_callback:
        progress_callback(0, 100)

    if gui_log:
        gui_log("Инициализация создания потенциального топа...", update_last=True)

    lost_path = os.path.join(os.path.dirname(__file__), "..", "csv", "lost_scores.csv")
    if not os.path.exists(lost_path):
        gui_log("Файл lost_scores.csv не найден. Прерываю создание потенциального топа.", update_last=False)
        return

    start = time.time()
    gui_log("Создаю потенциальный топ...", update_last=False)

    db_init()
    token = token_osu()

                            
    if progress_callback:
        progress_callback(10, 100)

    user_json = user_osu(user_identifier, lookup_key, token)
    if not user_json:
        gui_log(f"Ошибка: Не удалось получить данные пользователя '{user_identifier}' (тип: {lookup_key}).", False)
        raise ValueError(f"Пользователь не найден: {user_identifier}")
    username = user_json["username"]
    user_id = user_json["id"]
    gui_log(f"Получена информация о пользователе: {username}", update_last=False)

                                         
    if progress_callback:
        progress_callback(30, 100)

    stats = user_json.get("statistics", {})
    overall_pp = stats.get("pp", 0)
    overall_acc_from_api = float(stats.get("hit_accuracy", 0.0))


    if gui_log:
        gui_log("Запрашиваю топ-результаты...", update_last=False)
    if progress_callback:
        progress_callback(50, 100)

    raw_top = top_osu(token, user_id, limit=200)
    top_data = parse_top(raw_top, token)
    top_data = calc_weight(top_data)

    total_weight_pp = sum(item["weight_PP"] for item in top_data)
    diff = overall_pp - total_weight_pp

                                         
    if gui_log:
        gui_log("Сохраняю CSV (parsed_top.csv)...", update_last=False)
    if progress_callback:
        progress_callback(70, 100)

    parsed_file = os.path.join(os.path.dirname(__file__), "..", "csv", "parsed_top.csv")

    table_fields = [
        "PP", "Beatmap ID", "Beatmap", "Mods", "100", "50", "Misses",
        "Accuracy", "Score", "Date", "weight_%", "weight_PP", "Score ID", "Rank"
    ]
    rows_list = []
    for row in top_data:
        new_row = {
            "PP": row["PP"],
            "Beatmap ID": row["Beatmap ID"],
            "Beatmap": row["Beatmap"],
            "Mods": row["Mods"],
            "100": row["100"],
            "50": row["50"],
            "Misses": row["Misses"],
            "Accuracy": row["Accuracy"],
            "Score": row.get("Score", ""),
            "Date": row.get("Score Date", ""),
            "weight_%": row.get("weight_%", ""),
            "weight_PP": row.get("weight_PP", ""),
            "Score ID": row["Score ID"],
            "Rank": row["Rank"]
        }
        rows_list.append(new_row)

    with open(parsed_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=table_fields)
        writer.writeheader()
        for row in rows_list:
            writer.writerow(row)
        f.write("\n")

        summary_data = [
            ("Sum weight_PP", round(total_weight_pp)),
            ("Overall PP", round(overall_pp)),
            ("Difference", round(diff)),
            ("Overall Accuracy", f"{round(overall_acc_from_api, 2)}%")
        ]

        csv_writer = csv.writer(f)
        for label, val in summary_data:
            csv_writer.writerow([label, val])

                                           
    if gui_log:
        gui_log("Объединяю с потерянными скорами...", update_last=False)
    if progress_callback:
        progress_callback(90, 100)

    with open(lost_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        lost_scores = [r for r in reader]

    top_dict = {}
    for entry in top_data:
        try:
            bid = int(entry["Beatmap ID"])
        except Exception:
            continue
        if bid in top_dict:
            if entry["PP"] > top_dict[bid]["PP"]:
                top_dict[bid] = entry
        else:
            top_dict[bid] = entry

    for lost in lost_scores:
        try:
            bid = int(lost["Beatmap ID"])
        except Exception:
            continue
        lost_entry = {
            "PP": int(round(float(lost["PP"]))),
            "Beatmap ID": bid,
            "Status": "ranked",
            "Beatmap": lost["Beatmap"],
            "Mods": lost["Mods"] if lost["Mods"] else "NM",
            "100": lost["100"],
            "50": lost["50"],
            "Misses": lost["Misses"],
            "Accuracy": lost["Accuracy"],
            "Score": lost.get("Score", ""),
            "Date": lost.get("score_time", "") or lost.get("Date", ""),
            "weight_%": "",
            "weight_PP": "",
            "Score ID": "LOST",
            "Rank": lost["Rank"]
        }
        if bid in top_dict:
            if lost_entry["PP"] > top_dict[bid]["PP"]:
                top_dict[bid] = lost_entry
        else:
            top_dict[bid] = lost_entry

    combined = list(top_dict.values())
    combined.sort(key=lambda x: x["PP"], reverse=True)
    top_with_lost = combined[:200]
    top_with_lost = calc_weight(top_with_lost)

    total_weight_pp_new = sum(item["weight_PP"] for item in top_with_lost)
    pot_pp = total_weight_pp_new + diff
    diff_lost = pot_pp - overall_pp

    tot_weight_lost = 0
    acc_sum_lost = 0
    ranked_lost = sorted(top_with_lost, key=lambda x: x["PP"], reverse=True)
    for i, entry in enumerate(ranked_lost):
        mult = 0.95 ** i
        tot_weight_lost += mult
        acc_sum_lost += float(entry["Accuracy"]) * mult
    overall_acc_lost = acc_sum_lost / tot_weight_lost if tot_weight_lost else 0
    delta_acc = overall_acc_lost - overall_acc_from_api

    summary_rows = [
        {"PP": "Total weight_PP", "Beatmap ID": "", "Status": "", "Beatmap": "", "Mods": "", "Score": "", "100": "",
         "50": "", "Misses": "", "Accuracy": "", "Date": "", "weight_%": "", "weight_PP": round(total_weight_pp_new),
         "Score ID": ""},
        {"PP": "Overall Potential PP", "Beatmap ID": "", "Status": "", "Beatmap": "", "Mods": "", "Score": "",
         "100": "", "50": "", "Misses": "", "Accuracy": "", "Date": "", "weight_%": "",
         "weight_PP": round(pot_pp), "Score ID": ""},
        {"PP": "Difference", "Beatmap ID": "", "Status": "", "Beatmap": "", "Mods": "", "Score": "", "100": "",
         "50": "", "Misses": "", "Accuracy": "", "Date": "", "weight_%": "", "weight_PP": round(diff_lost),
         "Score ID": ""},
        {"PP": "Overall Accuracy", "Beatmap ID": "", "Status": "", "Beatmap": "", "Mods": "", "Score": "", "100": "",
         "50": "", "Misses": "", "Accuracy": "", "Date": "", "weight_%": "",
         "weight_PP": f"{round(overall_acc_lost, 2)}%", "Score ID": ""},
        {"PP": "Δ Overall Accuracy", "Beatmap ID": "", "Status": "", "Beatmap": "", "Mods": "", "Score": "", "100": "",
         "50": "", "Misses": "", "Accuracy": "", "Date": "", "weight_%": "",
         "weight_PP": f"{'+' if delta_acc >= 0 else ''}{round(delta_acc, 2)}%", "Score ID": ""}
    ]

    top_with_lost_file = os.path.join(os.path.dirname(__file__), "..", "csv", "top_with_lost.csv")
    table_fields2 = [
        "PP", "Beatmap ID", "Status", "Beatmap", "Mods", "100", "50", "Misses",
        "Accuracy", "Score", "Date", "Rank", "weight_%", "weight_PP", "Score ID"
    ]
    prep_rows = []
    for row in top_with_lost:
        new_r = {
            "PP": row["PP"],
            "Beatmap ID": row["Beatmap ID"],
            "Status": row.get("Status", ""),
            "Beatmap": row["Beatmap"],
            "Mods": row["Mods"],
            "100": row["100"],
            "50": row["50"],
            "Misses": row["Misses"],
            "Accuracy": row["Accuracy"],
            "Score": row.get("Score", ""),
            "Date": row.get("Score Date", row.get("Date", "")),
            "weight_%": row.get("weight_%", ""),
            "weight_PP": row.get("weight_PP", ""),
            "Score ID": row["Score ID"],
            "Rank": row["Rank"]
        }
        prep_rows.append(new_r)

    with open(top_with_lost_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=table_fields2)
        writer.writeheader()
        for row in prep_rows:
            writer.writerow(row)
        f.write("\n")
        csv_writer = csv.writer(f)
        for label, val in [
            ("Sum weight_PP", round(total_weight_pp_new)),
            ("Overall Potential PP", round(pot_pp)),
            ("Difference", round(diff_lost)),
            ("Overall Accuracy", f"{round(overall_acc_lost, 2)}%"),
            ("Δ Overall Accuracy", f"{'+' if delta_acc >= 0 else ''}{round(delta_acc, 2)}%")
        ]:
            csv_writer.writerow([label, val])

    elapsed = time.time() - start
    gui_log(f"Потенциальный топ создан за {elapsed:.2f} сек.", update_last=False)

                     
    if progress_callback:
        progress_callback(100, 100)