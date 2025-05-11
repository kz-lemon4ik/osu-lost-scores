import os
import csv
import calendar
import time
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from file_parser import FileParser
from database import db_init, db_get, db_save
from config import CUTOFF_DATE, THREAD_POOL_SIZE, CSV_DIR, IO_THREAD_POOL_SIZE, MAPS_DIR
from utils import mask_path_for_log, process_in_batches, track_parallel_progress

file_parser = FileParser()
logger = logging.getLogger(__name__)


def batch_process_beatmap_statuses(
    beatmap_ids,
    osu_api_client,
    include_unranked=False,
    gui_log=None,
    progress_callback=None,
    base_progress=60,
):
    if not beatmap_ids:
        return {}

    logger.info(f"Batch processing {len(beatmap_ids)} unique beatmaps")
    if gui_log:
        gui_log(
            f"Batch processing {len(beatmap_ids)} unique beatmaps", update_last=True
        )

    def process_db_beatmap(beatmap_id):
        db_info = db_get(beatmap_id)

        need_api_update = False
        if not db_info:
            need_api_update = True
        elif (
            not include_unranked
            and db_info.get("status") == "unknown"
            and db_info.get("status") != "not_found"
        ):
            need_api_update = True

        return {
            "beatmap_id": beatmap_id,
            "db_info": db_info,
            "need_api_update": need_api_update,
        }

    db_results = {}
    ids_to_fetch = set()

    batch_size = min(500, len(beatmap_ids))

    with ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE) as executor:
        future_to_beatmap = {
            executor.submit(process_db_beatmap, bid): bid for bid in beatmap_ids
        }

        completed = 0
        total = len(beatmap_ids)

        for future in as_completed(future_to_beatmap):
            completed += 1

            try:
                result = future.result()
                beatmap_id = result["beatmap_id"]

                if result["need_api_update"]:
                    ids_to_fetch.add(beatmap_id)
                else:
                    db_results[beatmap_id] = result["db_info"]

                if completed % 10 == 0 or completed == total:
                    if progress_callback:
                        progress_callback(
                            base_progress + int((completed / total) * 10), 100
                        )
            except Exception as e:
                logger.error(f"Error processing database entry: {e}")

    if ids_to_fetch:
        logger.info(f"Need to fetch {len(ids_to_fetch)} beatmaps from API")
        if gui_log:
            gui_log(
                f"Need to fetch {len(ids_to_fetch)} beatmaps from API", update_last=True
            )

        def process_api_beatmap(beatmap_id):
            try:
                info_api = osu_api_client.map_osu(beatmap_id)

                if info_api and isinstance(info_api, dict) and "status" in info_api:
                    db_save(
                        beatmap_id,
                        info_api["status"],
                        info_api["artist"],
                        info_api["title"],
                        info_api["version"],
                        info_api["creator"],
                        info_api.get("hit_objects", 0),
                    )
                    return {"beatmap_id": beatmap_id, "info": info_api}
                else:
                    db_info = {
                        "status": "not_found",
                        "artist": "",
                        "title": "",
                        "version": "",
                        "creator": "",
                        "hit_objects": 0,
                    }
                    db_save(
                        beatmap_id,
                        db_info["status"],
                        db_info["artist"],
                        db_info["title"],
                        db_info["version"],
                        db_info["creator"],
                        0,
                    )
                    return {"beatmap_id": beatmap_id, "info": db_info}
            except Exception as e:
                logger.error(f"Error fetching API data for beatmap {beatmap_id}: {e}")
                return {"beatmap_id": beatmap_id, "error": True}

        api_max_workers = min(5, THREAD_POOL_SIZE)
        total_api = len(ids_to_fetch)

        with ThreadPoolExecutor(max_workers=api_max_workers) as executor:
            future_to_api = {
                executor.submit(process_api_beatmap, bid): bid for bid in ids_to_fetch
            }

            api_results = track_parallel_progress(
                future_to_api,
                total_api,
                progress_callback=progress_callback,
                gui_log=gui_log,
                progress_message="Fetching data for map",
                start_progress=base_progress + 10,
                progress_range=10,
            )

            for result in api_results:
                if result and "error" not in result and "beatmap_id" in result:
                    beatmap_id = result["beatmap_id"]
                    db_results[beatmap_id] = result["info"]

    return db_results


def find_lost_scores(scores):
    if not scores:
        logger.warning("Empty score list in find_lost_scores")
        return []

    logger.debug("find_lost_scores received %d scores for analysis.", len(scores))

    def validate_and_preprocess_score(rec):
        try:
            if not isinstance(rec, dict):
                logger.warning("Score is not a dictionary: %s", type(rec))
                return None

            if "beatmap_id" not in rec or rec["beatmap_id"] is None:
                logger.debug("Skipping score due to missing beatmap_id: %s", rec)
                return None

            if not all(key in rec for key in ["mods", "pp", "total_score"]):
                logger.warning(
                    "Score does not contain all required keys: %s", rec.keys()
                )
                return None

            rec_copy = rec.copy()

            try:
                rec_copy["pp_float"] = float(rec_copy["pp"])
            except (ValueError, TypeError):
                logger.warning("Failed to convert PP to number: %s", rec_copy.get("pp"))
                rec_copy["pp_float"] = 0.0

            try:
                rec_copy["total_int"] = int(rec_copy["total_score"])
            except (ValueError, TypeError):
                logger.warning(
                    "Failed to convert total_score to number: %s",
                    rec_copy.get("total_score"),
                )
                rec_copy["total_int"] = 0

            return rec_copy
        except Exception as e:
            logger.warning("Error checking score: %s", e)
            return None

    processed_scores = process_in_batches(
        scores,
        batch_size=min(500, len(scores)),
        max_workers=IO_THREAD_POOL_SIZE,
        process_func=validate_and_preprocess_score,
    )

    valid_scores = [score for score in processed_scores if score is not None]

    if not valid_scores:
        logger.warning("No valid scores for analysis")
        return []

    groups = {}
    for rec in valid_scores:
        try:
            key = (rec["beatmap_id"], tuple(rec["mods"]))
            groups.setdefault(key, []).append(rec)
        except Exception as e:
            logger.warning("Error grouping score: %s", e)
            continue

    def process_score_group(group_data):
        key, recs = group_data
        try:
            if len(recs) < 2:
                return None

            best_pp = max(recs, key=lambda s: s["pp_float"])
            best_total = max(recs, key=lambda s: s["total_int"])

            if not all(k in best_pp for k in ["total_score", "pp", "beatmap_id"]):
                logger.warning("best_pp does not contain required keys")
                return None

            if not all(k in best_total for k in ["total_score", "pp"]):
                logger.warning("best_total does not contain required keys")
                return None

            pp_better = best_pp["pp_float"] > best_total["pp_float"]
            score_worse = best_pp["total_int"] < best_total["total_int"]

            if score_worse and pp_better:
                return (best_pp["beatmap_id"], best_pp)
            return None
        except Exception as e:
            logger.warning("Error processing score group: %s", e)
            return None

    possible_lost_results = process_in_batches(
        list(groups.items()),
        batch_size=min(500, len(groups)),
        max_workers=IO_THREAD_POOL_SIZE,
        process_func=process_score_group,
    )

    possible_lost = {}
    for result in possible_lost_results:
        if result is not None:
            bid, score = result
            possible_lost.setdefault(bid, []).append(score)

    map_scores = {}
    for rec in valid_scores:
        map_scores.setdefault(rec["beatmap_id"], []).append(rec)

    def finalize_lost_score(item):
        bid, candidates = item
        try:
            if not candidates:
                return None

            candidate = max(candidates, key=lambda s: s["pp_float"])
            all_scores = map_scores.get(bid, [])

            if not all_scores:
                return None

            best_score = max(all_scores, key=lambda s: s["pp_float"])

            if candidate["pp_float"] >= best_score["pp_float"]:
                return candidate
            return None
        except Exception as e:
            logger.warning("Error processing potentially lost score: %s", e)
            return None

    final_results = process_in_batches(
        list(possible_lost.items()),
        batch_size=min(500, len(possible_lost)),
        max_workers=IO_THREAD_POOL_SIZE,
        process_func=finalize_lost_score,
    )

    lost_results = [result for result in final_results if result is not None]

    try:
        lost_results.sort(key=lambda s: s["pp_float"], reverse=True)
    except Exception as e:
        logger.warning("Error sorting results: %s", e)

    return lost_results


def parse_top(raw, osu_api_client):
    calc_acc = file_parser.get_calc_acc()

    def format_date(iso_str):
        if not iso_str:
            return ""
        try:
            dt = datetime.datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
            return dt.strftime("%d-%m-%Y %H-%M-%S")
        except Exception:
            return iso_str

    def process_score(score):
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
                return None

            artist = beatmapset.get("artist", "")
            title = beatmapset.get("title", "")
            creator = beatmapset.get("creator", "")
            version = beatmap.get("version", "")
            full_name = f"{artist} - {title} ({creator}) [{version}]"

            status = beatmap.get("status", "unknown")
            rank = score.get("rank", "")

            return {
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
                "Rank": rank,
            }
        except Exception as e:
            logger.exception("Error in top result: %s", e)
            return None

    processed_scores = process_in_batches(
        raw,
        batch_size=min(200, len(raw)),
        max_workers=IO_THREAD_POOL_SIZE,
        process_func=process_score,
    )

    parsed = [score for score in processed_scores if score is not None]

    return parsed


def calc_weight(data):
    ranked = sorted(data, key=lambda x: x["PP"], reverse=True)
    for i, entry in enumerate(ranked):
        mult = 0.95**i
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


def scan_replays(
    game_dir,
    user_identifier,
    lookup_key,
    progress_callback=None,
    gui_log=None,
    include_unranked=False,
    osu_api_client=None,
):
    if not osu_api_client:
        raise ValueError("API client not provided")

    if progress_callback:
        progress_callback(0, 100)
    if gui_log:
        gui_log("Initializing...", update_last=True)

    logger.debug(
        "scan_replays called with: game_dir=%s, user_identifier=%s, lookup_key=%s, include_unranked=%s",
        mask_path_for_log(game_dir),
        user_identifier,
        lookup_key,
        include_unranked,
    )

    if not os.path.isdir(game_dir):
        error_msg = f"Game directory does not exist: {mask_path_for_log(game_dir)}"
        logger.error("Game directory does not exist: %s", mask_path_for_log(game_dir))
        if gui_log:
            gui_log(error_msg, False)
        raise ValueError(error_msg)

    songs = os.path.join(game_dir, "Songs")
    replays = os.path.join(game_dir, "Data", "r")

    file_parser.set_osu_base_path(game_dir)

    if not os.path.isdir(songs):
        error_msg = f"Songs directory not found: {mask_path_for_log(songs)}"
        logger.error("Songs directory not found: %s", mask_path_for_log(songs))
        if gui_log:
            gui_log(error_msg, False)
        raise ValueError(error_msg)

    logger.info("Starting .osu file scan in: %s", mask_path_for_log(songs))

    if not os.path.isdir(replays):
        error_msg = f"Replays directory not found: {mask_path_for_log(replays)}"
        logger.error("Replays directory not found: %s", mask_path_for_log(replays))
        if gui_log:
            gui_log(error_msg, False)
        raise ValueError(error_msg)

    try:
        db_init()
    except Exception as e:
        error_msg = f"Database initialization error: {e}"
        logger.exception("Database initialization error:")
        if gui_log:
            gui_log(error_msg, False)
        raise

    try:
        user_json = osu_api_client.user_osu(user_identifier, lookup_key)
        if not user_json:
            error_msg = f"Error: Failed to get user data '{user_identifier}' (type: {lookup_key})."
            logger.error(
                "Error: Failed to get user data '%s' (type: %s).",
                user_identifier,
                lookup_key,
            )

            if gui_log:
                gui_log(error_msg, False)
            raise ValueError(f"User not found: {user_identifier}")

        username = user_json["username"]
        user_id = user_json["id"]

        profile_link = f"https://osu.ppy.sh/users/{user_id}"
        logger.info("User found: %s (ID: %s)", username, user_id)
        if gui_log:
            gui_log(f"User found: {username} ({profile_link})", False)
    except Exception as e:
        error_msg = f"Error getting user data: {e}"
        logger.exception("Error getting user data:")
        if gui_log:
            gui_log(error_msg, False)
        raise

    gui_log("Scanning .osu files in Songs: 0%", update_last=True)
    last_songs_update = {"time": 0}

    def update_songs(curr, tot):
        now = time.time()
        if now - last_songs_update["time"] >= 1 or curr == tot:
            last_songs_update["time"] = now
            pct = int((curr / tot) * 100)
            if gui_log:
                gui_log(f"Scanning .osu files in Songs: {pct}%", update_last=True)

            if progress_callback:
                progress_callback(int(pct * 0.1), 100)                    

    md5_map = file_parser.find_osu(songs, progress_callback=update_songs)

    if progress_callback:
        progress_callback(10, 100)                

    gui_log("Scanning .osu files in Songs: 100%", update_last=True)
    gui_log(f"{len(md5_map)} osu files found in Songs.", update_last=False)
    cutoff = CUTOFF_DATE

    rep_files = [f for f in os.listdir(replays) if f.endswith(".osr")]
    total_rep = len(rep_files)
    gui_log(f"Processed 0/{total_rep} replays", update_last=True)

    logger.info(
        "Starting .osr file scan in: %s for user: %s",
        mask_path_for_log(replays),
        username,
    )

    start = time.time()

                                                               
    if gui_log:
        gui_log("Phase 1: Preliminary scanning of replays...", update_last=False)

                                     
    replay_data_list = []
                                                       
    md5_to_lookup = set()

    count = 0
    last_replay_update = {"time": 0}

    def update_replay_phase1(curr, tot):
        if progress_callback:
            progress_callback(10 + int((curr / tot) * 20), 100)                 

    with ThreadPoolExecutor(max_workers=IO_THREAD_POOL_SIZE) as executor:
        futures = {
            executor.submit(
                file_parser.parse_osr_info, os.path.join(replays, f), username
            ): f
            for f in rep_files
        }

        for fut in as_completed(futures):
            count += 1
            update_replay_phase1(count, total_rep)
            osr_filename = futures[fut]

            try:
                replay_data = fut.result()
                if replay_data:
                                                                  
                    if replay_data["beatmap_md5"] not in md5_map:
                                                                   
                        if (
                            replay_data["beatmap_md5"]
                            not in file_parser.not_submitted_cache
                        ):
                                                                    
                            md5_to_lookup.add(replay_data["beatmap_md5"])
                                                
                    replay_data_list.append(replay_data)
            except Exception as e:
                logger.exception(
                    "Error in Phase 1 processing replay %s: %s", osr_filename, e
                )

            now = time.time()
            if now - last_replay_update["time"] >= 1 or count == total_rep:
                last_replay_update["time"] = now
                gui_log(
                    f"Phase 1: Processed {count}/{total_rep} replays", update_last=True
                )

                                                      
    if gui_log:
        gui_log(
            f"Phase 2: Looking up {len(md5_to_lookup)} unique beatmap IDs...",
            update_last=False,
        )

    if progress_callback:
        progress_callback(30, 100)                    

                                                   
    md5_results = {}

                                                            
    md5_list = list(md5_to_lookup)
    total_md5 = len(md5_list)

    for i, md5 in enumerate(md5_list):
        try:
            if gui_log and (i % 1 == 0 or i == total_md5 - 1):
                gui_log(f"Looking up beatmap ID {i + 1}/{total_md5}", update_last=True)

            if progress_callback:
                progress_callback(30 + int((i / total_md5) * 15), 100)                 

            beatmap_id = osu_api_client.lookup_osu(md5)
            md5_results[md5] = beatmap_id

                                                      
            if beatmap_id is None:
                with file_parser.osr_cache_lock:
                    file_parser.not_submitted_cache[md5] = True
                    if i % 10 == 0 or i == total_md5 - 1:                     
                        file_parser.not_submitted_cache_save(
                            file_parser.not_submitted_cache
                        )

        except Exception as e:
            logger.exception(f"Error looking up MD5 {md5}: {e}")
            md5_results[md5] = None

                                                   
    file_parser.not_submitted_cache_save(file_parser.not_submitted_cache)

                                                       
    if gui_log:
        gui_log("Phase 3: Downloading missing .osu files...", update_last=False)

    if progress_callback:
        progress_callback(45, 100)                    

                                              
    download_ids = [bid for md5, bid in md5_results.items() if bid is not None]
    total_downloads = len(download_ids)
    downloads_completed = 0

    every_n = max(1, total_rep // 10)                                              
    logger.debug(
        "Processing %d beatmap MD5 to ID mappings for download", len(md5_results)
    )
    for i, (md5, beatmap_info) in enumerate(md5_results.items()):
        if beatmap_info is not None and isinstance(beatmap_info, dict):
            actual_beatmap_id = beatmap_info.get("id")
            if actual_beatmap_id:
                try:
                    if gui_log:
                        downloads_completed += 1
                        gui_log(
                            f"Downloading missing maps {downloads_completed}/{total_downloads}",
                            update_last=True,
                        )

                    if progress_callback and total_downloads > 0:
                        progress_callback(
                            45 + int((downloads_completed / total_downloads) * 15), 100
                        )

                    maps_dir_files = [
                        f for f in os.listdir(MAPS_DIR) if f.endswith(".osu")
                    ]
                    found_in_maps = False
                    osu_file_path_for_md5 = None

                    for maps_file in maps_dir_files:
                        file_path = os.path.join(MAPS_DIR, maps_file)
                        try:
                            file_md5 = file_parser.get_md5(file_path)
                            if file_md5 == md5:
                                osu_file_path_for_md5 = file_path
                                logger.info(
                                    "Found existing .osu file in MAPS_DIR for md5 %s: %s",
                                    md5,
                                    mask_path_for_log(osu_file_path_for_md5),
                                )
                                found_in_maps = True
                                break
                        except Exception as e:
                            logger.warning(
                                f"Error checking file MD5 {mask_path_for_log(file_path)}: {e}"
                            )

                    if not found_in_maps and md5 in md5_map:
                        osu_file_path_for_md5 = md5_map[md5]
                        logger.info(
                            "Found existing .osu file in Songs for md5 %s: %s",
                            md5,
                            mask_path_for_log(osu_file_path_for_md5),
                        )
                        found_in_maps = True

                    if not found_in_maps:
                        new_osu_path = file_parser.download_osu_file(
                            actual_beatmap_id, osu_api_client
                        )
                        if new_osu_path:
                            osu_file_path_for_md5 = new_osu_path
                            md5_map[md5] = new_osu_path
                            file_parser.update_osu_md5_cache(new_osu_path, md5)
                            logger.info(
                                "Downloaded new .osu file for beatmap_id %s by md5 %s",
                                actual_beatmap_id,
                                md5,
                            )
                        else:
                            logger.warning(
                                f"Download failed for beatmap_id {actual_beatmap_id}"
                            )

                    if osu_file_path_for_md5 and md5 not in md5_map:
                        md5_map[md5] = osu_file_path_for_md5

                except Exception as e:
                    logger.exception(
                        f"Error processing download/check for beatmap_id {actual_beatmap_id}: {e}"
                    )
            else:
                logger.warning(
                    f"Lookup for md5 {md5} returned data but no beatmap ID: {beatmap_info}"
                )

                                      
    if gui_log:
        gui_log("Phase 4: Calculating PP values...", update_last=False)

    if progress_callback:
        progress_callback(60, 100)                    

    score_list = []
    total_replays = len(replay_data_list)

    count = 0
    last_pp_update = {"time": 0}

    def update_pp_progress(curr, tot):
        if progress_callback:
            progress_callback(60 + int((curr / tot) * 10), 100)                 

    with ThreadPoolExecutor(max_workers=IO_THREAD_POOL_SIZE) as executor:
        futures = {
            executor.submit(
                file_parser.process_osr_with_path,
                replay_data,
                md5_map,
                file_parser.not_submitted_cache,
                osu_api_client,
            ): replay_data
            for replay_data in replay_data_list
        }

                                                                                    
        results = track_parallel_progress(
            futures,
            total_replays,
            progress_callback=update_pp_progress,
            gui_log=gui_log,
            progress_message="Phase 4: Calculated PP for",
            start_progress=0,                                                     
            progress_range=100,                                                     
        )

                                 
        for res in results:
            if res is not None:
                score_list.append(res)

                
    file_parser.osr_save(file_parser.osr_cache)

    elapsed = time.time() - start
    logger.info(
        "Replay scanning completed in %.2f sec. %d scores found",
        elapsed,
        len(score_list),
    )
    gui_log(
        f"Replay scanning completed in {elapsed:.2f} sec. {len(score_list)} found results.",
        update_last=False,
    )

    if gui_log:
        gui_log("Processing lost scores...", update_last=False)
    if progress_callback:
        progress_callback(70, 100)                                           

    lost = find_lost_scores(score_list)
    lost = [
        r
        for r in lost
        if calendar.timegm(time.strptime(r["score_time"], "%d-%m-%Y %H-%M-%S")) < cutoff
    ]
    logger.info("%d lost scores found (before cutoff)", len(lost))

    logger.info("Include unranked/loved beatmaps: %s", include_unranked)

    if include_unranked:
        logger.info(
            "ENABLED unranked/loved maps. Getting information locally. Total scores: %d",
            len(lost),
        )

                                                             
        def process_beatmap(item):
            beatmap_id, rec = item
            result = {"beatmap_id": beatmap_id, "status": "unknown"}

            osu_file_path = rec.get("osu_file_path")
            if not osu_file_path or not os.path.exists(osu_file_path):
                return result

            try:
                                                             
                count_objs = file_parser.count_objs
                parse_osu_metadata = file_parser.parse_osu_metadata

                db_info = db_get(beatmap_id)
                result["db_info"] = db_info

                if not db_info or db_info.get("status") == "unknown":
                    hit_objects = count_objs(osu_file_path, beatmap_id)
                    metadata = parse_osu_metadata(osu_file_path)

                    status = db_info.get("status", "unknown") if db_info else "unknown"

                    db_save(
                        beatmap_id,
                        status,
                        metadata.get("artist", rec.get("artist", "")),
                        metadata.get("title", rec.get("title", "")),
                        metadata.get("version", rec.get("version", "")),
                        metadata.get("creator", rec.get("creator", "")),
                        hit_objects,
                    )

                    result.update(
                        {
                            "processed": True,
                            "hit_objects": hit_objects,
                            "metadata": metadata,
                        }
                    )

                return result
            except Exception as e:
                logger.exception(f"Error processing beatmap {beatmap_id}: {e}")
                return {"beatmap_id": beatmap_id, "error": str(e)}

                                            
        beatmap_tasks = []
        for rec in lost:
            beatmap_id = rec.get("beatmap_id")
            if not beatmap_id:
                continue

            beatmap_tasks.append((beatmap_id, rec))

                                        
        beatmap_results = {}
        total_tasks = len(beatmap_tasks)

        if total_tasks > 0:
            logger.info(f"Processing {total_tasks} local .osu files in parallel")

            with ThreadPoolExecutor(max_workers=IO_THREAD_POOL_SIZE) as executor:
                futures = {
                    executor.submit(process_beatmap, task): task
                    for task in beatmap_tasks
                }

                                                                                            
                results = track_parallel_progress(
                    futures,
                    total_tasks,
                    progress_callback=progress_callback,
                    gui_log=gui_log,
                    progress_message="Processing local map data",
                    start_progress=70,
                    progress_range=10,
                )

                                         
                for result in results:
                    if result and "beatmap_id" in result:
                        beatmap_id = result["beatmap_id"]
                        beatmap_results[beatmap_id] = result

                                    
        def update_record_status(rec):
            rec_copy = rec.copy()
            rec_copy["Status"] = "unknown"
            return rec_copy

        with ThreadPoolExecutor(max_workers=IO_THREAD_POOL_SIZE) as executor:
            updated_lost = list(executor.map(update_record_status, lost))

        lost = updated_lost

        if gui_log and len(beatmap_results) == 0:
            gui_log("No local files found for processing", update_last=True)

        logger.info("ENABLED unranked/loved maps. Total scores: %d", len(lost))

    else:
        logger.info(f"Checking status for {len(lost)} maps...")

                                        
        unique_beatmap_ids = set(
            rec["beatmap_id"] for rec in lost if "beatmap_id" in rec
        )

        if gui_log:
            gui_log(f"Checking status for {len(lost)} maps...", update_last=False)

                                            
        db_results = batch_process_beatmap_statuses(
            unique_beatmap_ids,
            osu_api_client,
            include_unranked,
            gui_log=gui_log,
            progress_callback=progress_callback,
            base_progress=80,
        )

                                            
        for i, rec in enumerate(lost):
            if "beatmap_id" in rec:
                db_ = db_results.get(rec["beatmap_id"], {})
                rec["Status"] = db_.get("status", "unknown")

                                                                        
                if i % 5 == 0 or i == len(lost) - 1:
                    if gui_log:
                        gui_log(
                            f"Getting information about map {rec['beatmap_id']} ({i + 1}/{len(lost)})",
                            update_last=True,
                        )
                    if progress_callback:
                        progress_callback(80 + int((i / len(lost)) * 10), 100)

        original_count = len(lost)
        lost = [
            r
            for r in lost
            if r.get("Status") in ["ranked", "approved"]
            and r.get("Status") != "not_found"
        ]
        filtered_count = len(lost)
        logger.info(
            f"Filtered {original_count - filtered_count} scores, remaining: {filtered_count}"
        )

    for rec in lost:
        db_info = db_get(rec["beatmap_id"])
        if not db_info or not db_info.get("hit_objects", 0):
            pass

    if gui_log:
        gui_log("Saving results...", update_last=True)
    if progress_callback:
        progress_callback(90, 100)                                            

    if lost:
        out_file = os.path.join(CSV_DIR, "lost_scores.csv")
        fields = [
            "PP",
            "Beatmap ID",
            "Beatmap",
            "Mods",
            "100",
            "50",
            "Misses",
            "Accuracy",
            "Score",
            "Date",
            "Rank",
        ]

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

                        rank_ = file_parser.grade_osu(
                            rec["beatmap_id"], c300, c100, c50, cMiss
                        )

                        writer.writerow(
                            {
                                "PP": rec["pp"],
                                "Beatmap ID": rec["beatmap_id"],
                                "Beatmap": f"{rec.get('artist', '')} - {rec.get('title', '')} ({rec.get('creator', '')}) [{rec.get('version', '')}]",
                                "Mods": ", ".join(file_parser.sort_mods(rec["mods"]))
                                if rec["mods"]
                                else "NM",
                                "100": c100,
                                "50": c50,
                                "Misses": cMiss,
                                "Accuracy": rec["Accuracy"],
                                "Score": rec.get("total_score", ""),
                                "Date": rec.get("score_time", ""),
                                "Rank": rank_,
                            }
                        )
                gui_log("File lost_scores.csv saved.", update_last=True)
                break
            except PermissionError:
                logger.warning(
                    "File %s is busy, retrying in 0.5 sec.", mask_path_for_log(out_file)
                )
                time.sleep(0.5)
            except Exception as e:
                logger.exception("Error writing %s: %s", mask_path_for_log(out_file), e)
                break
    else:
        logger.info("Empty: lost scores not written.")

    if progress_callback:
        progress_callback(100, 100)


def make_top(
    game_dir,
    user_identifier,
    lookup_key,
    gui_log=None,
    progress_callback=None,
    osu_api_client=None,
):
    if not osu_api_client:                     
        raise ValueError("API client not provided")

    if progress_callback:
        progress_callback(0, 100)

    if gui_log:
        gui_log("Initializing potential top creation...", update_last=True)

    logger.debug(
        "make_top called with: game_dir=%s, user_identifier=%s, lookup_key=%s",
        mask_path_for_log(game_dir),
        user_identifier,
        lookup_key,
    )

    lost_path = os.path.join(CSV_DIR, "lost_scores.csv")
    if not os.path.exists(lost_path):
        error_message = (
            "File lost_scores.csv not found. Aborting potential top creation."
        )
        logger.error(error_message)
        if gui_log:
            gui_log(error_message, update_last=False)
        return

    start = time.time()
    gui_log("Creating potential top...", update_last=False)

    db_init()

    if progress_callback:
        progress_callback(10, 100)

    user_json = osu_api_client.user_osu(user_identifier, lookup_key)
    if not user_json:
        gui_log(
            f"Error: Failed to get user data '{user_identifier}' (type: {lookup_key}).",
            False,
        )
        raise ValueError(f"User not found: {user_identifier}")
    username = user_json["username"]
    user_id = user_json["id"]
    gui_log(f"User information received: {username}", update_last=False)

    if progress_callback:
        progress_callback(30, 100)

    stats = user_json.get("statistics", {})
    overall_pp = stats.get("pp", 0)
    overall_acc_from_api = float(stats.get("hit_accuracy", 0.0))

    if gui_log:
        gui_log("Requesting top results...", update_last=False)
    if progress_callback:
        progress_callback(50, 100)

    raw_top = osu_api_client.top_osu(user_id, limit=200)
    top_data = parse_top(raw_top, osu_api_client)
    top_data = calc_weight(top_data)

    total_weight_pp = sum(item["weight_PP"] for item in top_data)
    diff = overall_pp - total_weight_pp

    if gui_log:
        gui_log("Saving CSV (parsed_top.csv)...", update_last=False)
    if progress_callback:
        progress_callback(70, 100)

    parsed_file = os.path.join(CSV_DIR, "parsed_top.csv")

    table_fields = [
        "PP",
        "Beatmap ID",
        "Beatmap",
        "Mods",
        "100",
        "50",
        "Misses",
        "Accuracy",
        "Score",
        "Date",
        "weight_%",
        "weight_PP",
        "Score ID",
        "Rank",
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
            "Rank": row["Rank"],
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
            ("Overall Accuracy", f"{round(overall_acc_from_api, 2)}%"),
        ]

        csv_writer = csv.writer(f)
        for label, val in summary_data:
            csv_writer.writerow([label, val])

    if gui_log:
        gui_log("Merging with lost scores...", update_last=False)
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
            "Rank": lost["Rank"],
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
        mult = 0.95**i
        tot_weight_lost += mult
        acc_sum_lost += float(entry["Accuracy"]) * mult
    overall_acc_lost = acc_sum_lost / tot_weight_lost if tot_weight_lost else 0
    delta_acc = overall_acc_lost - overall_acc_from_api

    top_with_lost_file = os.path.join(CSV_DIR, "top_with_lost.csv")
    table_fields2 = [
        "PP",
        "Beatmap ID",
        "Status",
        "Beatmap",
        "Mods",
        "100",
        "50",
        "Misses",
        "Accuracy",
        "Score",
        "Date",
        "Rank",
        "weight_%",
        "weight_PP",
        "Score ID",
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
            "Rank": row["Rank"],
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
            (
                "Δ Overall Accuracy",
                f"{'+' if delta_acc >= 0 else ''}{round(delta_acc, 2)}%",
            ),
        ]:
            csv_writer.writerow([label, val])

    elapsed = time.time() - start
    logger.info("Potential top created in %.2f sec.", elapsed)
    gui_log(f"Potential top created in {elapsed:.2f} sec.", update_last=False)

    if progress_callback:
        progress_callback(100, 100)
