import calendar
from datetime import datetime
import logging
import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor

from app_config import (
    ANALYSIS_DIR,
    CUTOFF_DATE,
    IO_THREAD_POOL_SIZE,
    MAPS_DIR,
    IMAGES_DIR,
)
from database import db_get_map, db_init, db_update_from_api, db_upsert_from_scan
from file_parser import file_parser
from generate_image import create_summary_badge
from osu_api import OAuthSessionExpiredException
from path_utils import mask_path_for_log
from utils import (
    create_analysis_json_structure,
    process_in_batches,
    save_analysis_to_json,
    track_parallel_progress,
)

logger = logging.getLogger(__name__)
asset_downloads_logger = logging.getLogger("asset_downloads")


def find_lost_scores(scores, cutoff_date):
    if not scores:
        logger.warning("Empty score list in find_lost_scores")
        return [], 0

    logger.debug("find_lost_scores received %d scores for analysis", len(scores))

    def validate_and_preprocess_score(rec):
        try:
            if not isinstance(rec, dict) or not all(
                required_key in rec
                for required_key in ["mods", "pp", "total_score", "score_time"]
            ):
                return None
            rec_copy = rec.copy()
            rec_copy["pp_float"] = float(rec.get("pp", 0.0))
            rec_copy["total_int"] = int(rec.get("total_score", 0))
            map_identifier = rec.get("beatmap_md5")
            rec_copy["map_identifier"] = map_identifier
            if not map_identifier:
                return None
            rec_copy["timestamp"] = calendar.timegm(
                time.strptime(rec["score_time"], "%d-%m-%Y %H-%M-%S")
            )
            return rec_copy
        except (ValueError, TypeError) as e:
            logger.warning(
                "Could not preprocess score, skipping. Score: %s, Error: %s", rec, e
            )
            return None

    max_workers = min(IO_THREAD_POOL_SIZE, max(4, os.cpu_count() or 8))
    processed_scores = process_in_batches(
        scores,
        batch_size=min(2000, len(scores)),
        max_workers=max_workers,
        process_func=validate_and_preprocess_score,
    )
    valid_scores = [score for score in processed_scores if score is not None]

    groups_by_mod = {}
    scores_by_map = {}

    for score_record in valid_scores:
        key = (
            score_record["map_identifier"],
            tuple(sorted(score_record.get("mods", []))),
        )
        groups_by_mod.setdefault(key, []).append(score_record)
        scores_by_map.setdefault(score_record["map_identifier"], []).append(
            score_record
        )

    preliminary_lost_scores = []
    total_candidates_found = 0

    for group_key, group_scores in groups_by_mod.items():
        if len(group_scores) < 2:
            continue

        try:
            candidate_score = max(group_scores, key=lambda s: s["pp_float"])

            best_score_overall_in_group = max(
                group_scores, key=lambda s: s["total_int"]
            )
            if (
                candidate_score is not best_score_overall_in_group
                and candidate_score["pp_float"]
                > best_score_overall_in_group["pp_float"]
            ):
                total_candidates_found += 1

            scores_in_valid_range = [
                s for s in group_scores if s["timestamp"] < cutoff_date
            ]
            if not scores_in_valid_range:
                continue

            best_score_play_in_range = max(
                scores_in_valid_range, key=lambda s: s["total_int"]
            )

            if candidate_score is best_score_play_in_range:
                continue

            pp_is_better = (
                candidate_score["pp_float"] > best_score_play_in_range["pp_float"]
            )
            score_is_worse = (
                candidate_score["total_int"] < best_score_play_in_range["total_int"]
            )

            if (
                pp_is_better
                and score_is_worse
                and candidate_score["timestamp"] < cutoff_date
            ):
                preliminary_lost_scores.append(candidate_score)
        except (KeyError, ValueError, TypeError) as group_exc:
            logger.warning(f"Error processing score group {group_key}: {group_exc}")

    final_lost_results = []
    for candidate in preliminary_lost_scores:
        map_id = candidate["map_identifier"]
        all_scores_on_map = scores_by_map.get(map_id, [])
        if not all_scores_on_map:
            continue

        true_best_pp_on_map = max(all_scores_on_map, key=lambda s: s["pp_float"])

        if candidate is true_best_pp_on_map:
            final_lost_results.append(candidate)

    final_lost_results.sort(key=lambda s: s["pp_float"], reverse=True)

    return final_lost_results, total_candidates_found


def parse_top(raw):
    calc_acc = file_parser.get_calc_acc()

    def format_date(iso_str):
        if not iso_str:
            return ""
        try:
            dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
            return dt.strftime("%d-%m-%Y %H-%M-%S")
        except (ValueError, TypeError):
            return iso_str

    def process_score(score):
        try:
            beatmap_api_data = score.get("beatmap", {})
            beatmap_id = beatmap_api_data.get("id")
            if beatmap_id is None:
                return None

            map_db_data = db_get_map(beatmap_id, by="id")

            final_map_data = {}
            if map_db_data:
                final_map_data.update(map_db_data)

            final_map_data.update(score.get("beatmapset", {}))
            final_map_data.update(beatmap_api_data)

            full_name = f"{final_map_data.get('artist', '')} - {final_map_data.get('title', '')} ({final_map_data.get('creator', '')}) [{final_map_data.get('version', '')}]"

            stats = score.get("statistics", {})
            c100 = stats.get("count_100", 0)
            c50 = stats.get("count_50", 0)
            cmiss = stats.get("count_miss", 0)
            c300 = stats.get("count_300", 0)
            acc = calc_acc(c300, c100, c50, cmiss)

            return {
                "Score ID": score.get("id", ""),
                "PP": round(float(score.get("pp", 0))),
                "Beatmap ID": beatmap_id,
                "Beatmap MD5": final_map_data.get("md5_hash", ""),
                "Beatmap": full_name,
                "Mods": ", ".join(score.get("mods", [])) if score.get("mods") else "NM",
                "Score": score.get("score", 0),
                "100": c100,
                "50": c50,
                "Misses": cmiss,
                "Status": final_map_data.get("status", "unknown"),
                "Accuracy": acc,
                "Score Date": format_date(score.get("created_at", "")),
                "total_score": score.get("score", 0),
                "Rank": score.get("rank", ""),
            }
        except (ValueError, TypeError, KeyError) as e:
            logger.exception("Error processing single top score: %s", e)
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


def announce_phase_start(phase_key, phase_definitions, gui_log, phase_logger):
    phase_info = phase_definitions.get(phase_key)
    user_title = phase_info.get("user_title")
    technical_name = phase_info.get("name", phase_key)

    if gui_log:
        gui_log(user_title, update_last=True)
        gui_log(user_title, update_last=False)
    if phase_logger:
        phase_logger.info(f"--- {technical_name} ---")


def scan_replays(
    game_dir,
    user_identifier,
    lookup_key,
    progress_callback=None,
    progress_logger=None,
    gui_log=None,
    include_unranked=False,
    check_missing_ids=False,
    osu_api_client=None,
):
    if not osu_api_client:
        raise ValueError("API client not provided")

    summary_stats = {
        "maps_to_resolve": 0,
        "maps_resolved": 0,
        "maps_downloaded": 0,
        "maps_not_found_resolve": 0,
    }
    phase_definitions = {
        "init": {
            "name": "Initialization",
            "user_title": "Initializing...",
            "weight": 1,
        },
        "osu_scan": {
            "name": "Scanning .osu files",
            "user_title": "Scanning beatmap files...",
            "weight": 15,
        },
        "osr_parse": {
            "name": "Parsing local replays",
            "user_title": "Parsing local replays...",
            "weight": 5,
        },
        "resolve_missing": {
            "name": "Resolving missing maps",
            "user_title": "Resolving missing maps...",
            "weight": 20,
        },
        "precache_top": {
            "name": "Pre-caching top scores",
            "user_title": "Pre-caching top scores...",
            "weight": 2,
        },
        "pp_calc": {
            "name": "Calculating PP",
            "user_title": "Calculating PP...",
            "weight": 40,
        },
        "find_lost": {
            "name": "Finding lost scores",
            "user_title": "Finding lost scores...",
            "weight": 2,
        },
        "deferred_lookup": {
            "name": "Deferred map lookup",
            "user_title": "Looking up map details...",
            "weight": 8,
        },
        "validate_status": {
            "name": "Validating map statuses",
            "user_title": "Validating map statuses...",
            "weight": 7,
        },
        "saving": {
            "name": "Saving results",
            "user_title": "Saving results...",
            "weight": 1,
        },
    }

    progress_map = {}

    def report_progress(phase_key, current, total):
        if progress_callback and total > 0:
            base, range_percentage = progress_map.get(phase_key, (0, 0))
            overall_progress = base + (current / total) * range_percentage
            progress_callback(int(overall_progress), 100)

    if progress_callback:
        progress_callback(0, 100)

    announce_phase_start("init", phase_definitions, gui_log, phase_logger=logger)

    songs = os.path.join(game_dir, "Songs")
    replays_dir = os.path.join(game_dir, "Data", "r")
    if not os.path.isdir(songs) or not os.path.isdir(replays_dir):
        error_msg = f"Game directory '{mask_path_for_log(game_dir)}' is invalid or missing Songs/Data/r folders"
        if gui_log:
            gui_log(error_msg, update_last=False)
        raise ValueError(error_msg)

    file_parser.set_osu_base_path(game_dir)
    db_init()

    try:
        user_json = osu_api_client.user_osu(user_identifier, lookup_key)
        if not user_json:
            raise ValueError(f"User not found: {user_identifier}")
        username, user_id = user_json["username"], user_json["id"]
        if gui_log:
            gui_log(
                f"User found: {username} (https://osu.ppy.sh/users/{user_id})", False
            )
    except OAuthSessionExpiredException:
        logger.warning(
            "OAuth session expired while getting user data for %s", user_identifier
        )
        raise
    except requests.exceptions.RequestException as e:
        logger.exception("Failed to get user data for %s", user_identifier)
        if gui_log:
            gui_log(f"Error getting user data: {e}", False)
        raise

    start_time = time.time()
    all_replay_files = [f for f in os.listdir(replays_dir) if f.endswith(".osr")]
    summary_stats["total_replays"] = len(all_replay_files)

    all_possible_phases = [
        "osu_scan",
        "osr_parse",
        "resolve_missing",
        "precache_top",
        "pp_calc",
        "deferred_lookup",
        "validate_status",
    ]

    total_weight = sum(phase_definitions[p]["weight"] for p in all_possible_phases)
    current_progress_base = 0
    for key in all_possible_phases:
        weight = phase_definitions[key]["weight"]
        percentage = (weight / total_weight) * 100 if total_weight > 0 else 0
        progress_map[key] = (current_progress_base, percentage)
        current_progress_base += percentage

    all_replay_files = [f for f in os.listdir(replays_dir) if f.endswith(".osr")]
    summary_stats["total_replays"] = len(all_replay_files)

    announce_phase_start("osu_scan", phase_definitions, gui_log, phase_logger=logger)

    phase_key_osu_scan = "osu_scan"
    file_parser.find_osu(
        songs,
        progress_callback=lambda c, t: report_progress(phase_key_osu_scan, c, t),
        gui_log=gui_log,
        progress_logger=progress_logger,
    )

    announce_phase_start("osr_parse", phase_definitions, gui_log, phase_logger=logger)
    phase_key_osr_parse = "osr_parse"
    with ThreadPoolExecutor(max_workers=IO_THREAD_POOL_SIZE) as executor:
        futures = {
            executor.submit(
                file_parser.parse_osr_info, os.path.join(replays_dir, f), username
            ): f
            for f in all_replay_files
        }
        all_replay_data = [
            r
            for r in track_parallel_progress(
                futures,
                len(all_replay_files),
                progress_callback=lambda c, t: report_progress(
                    phase_key_osr_parse, c, t
                ),
                gui_log=gui_log,
                progress_logger=logger,
                log_interval_sec=5,
                progress_message="Parsing .osr files",
                gui_update_step=1000,
            )
            if r
        ]

    summary_stats["parsed_replays"] = len(all_replay_data)
    replays_with_osu, replays_missing_osu = [], []
    for r_data in all_replay_data:
        if r_data.get("beatmap_md5") and db_get_map(
            r_data.get("beatmap_md5"), by="md5"
        ):
            replays_with_osu.append(r_data)
        else:
            replays_missing_osu.append(r_data)

    replays_for_pp_calc = [(r, None) for r in replays_with_osu]

    announce_phase_start(
        "resolve_missing", phase_definitions, gui_log, phase_logger=logger
    )

    if check_missing_ids and replays_missing_osu:
        base_resolve, range_resolve = progress_map.get(
            "resolve_missing", (current_progress_base, 0)
        )
        md5_to_replays_map = {}
        for r_data in replays_missing_osu:
            md5 = r_data.get("beatmap_md5")
            if md5:
                md5_to_replays_map.setdefault(md5, []).append(r_data)

        unique_md5s_to_process = list(md5_to_replays_map.keys())
        total_md5s = len(unique_md5s_to_process)
        summary_stats["maps_to_resolve"] = total_md5s
        logger.info(f"Resolving {total_md5s} missing maps via API...")

        stats = {"resolved": 0, "downloaded": 0, "not_found": 0}
        last_log_time = time.time()

        for i, md5 in enumerate(unique_md5s_to_process):
            report_progress("resolve_missing", i + 1, total_md5s)
            progress_message = f"Resolving maps {i + 1}/{total_md5s}..."
            if gui_log:
                gui_log(progress_message, update_last=True)

            now = time.time()
            if now - last_log_time > 60 or (i + 1) == total_md5s:
                logger.info(progress_message)
                last_log_time = now

            try:
                lookup_result = osu_api_client.lookup_osu(md5)
                if lookup_result and "beatmap_id" in lookup_result:
                    stats["resolved"] += 1
                    beatmap_id = lookup_result["beatmap_id"]
                    target_save_path = os.path.join(
                        MAPS_DIR, f"beatmap_{beatmap_id}.osu"
                    )

                    new_path = osu_api_client.download_osu_file(
                        beatmap_id, target_save_path
                    )
                    if new_path:
                        stats["downloaded"] += 1
                        rel_path = file_parser.to_relative_path(new_path)
                        update_data = {
                            "file_path": rel_path,
                            "last_modified": int(os.path.getmtime(new_path)),
                            "beatmap_id": lookup_result.get("beatmap_id"),
                            "beatmapset_id": lookup_result.get("beatmapset_id"),
                            "artist": lookup_result.get("artist"),
                            "title": lookup_result.get("title"),
                            "creator": lookup_result.get("creator"),
                            "version": lookup_result.get("version"),
                            "api_status": lookup_result.get("api_status"),
                            "lookup_status": "found",
                        }
                        db_upsert_from_scan(md5, update_data)
                        for r_data in md5_to_replays_map[md5]:
                            replays_for_pp_calc.append((r_data, lookup_result))
                else:
                    stats["not_found"] += 1
            except (requests.exceptions.RequestException, IOError, OSError) as e:
                asset_downloads_logger.exception(
                    "Failed to resolve/download map for MD5 %s: %s", md5, e
                )

        summary_stats.update(
            {
                "maps_resolved": stats["resolved"],
                "maps_downloaded": stats["downloaded"],
                "maps_not_found_resolve": stats["not_found"],
            }
        )
        logger.info(
            f"Missing maps phase finished: {stats['resolved']} resolved, {stats['downloaded']} downloaded, {stats['not_found']} not found"
        )
        current_progress_base += range_resolve

    announce_phase_start(
        "precache_top", phase_definitions, gui_log, phase_logger=logger
    )
    try:
        top_scores = osu_api_client.top_osu(user_id, limit=200)
        if top_scores:
            unique_maps_to_cache = {
                (s["beatmap"]["id"], s["beatmapset"]["id"]): (
                    s["beatmap"],
                    s["beatmapset"],
                )
                for s in top_scores
                if s.get("beatmap") and s.get("beatmapset")
            }
            for beatmap, beatmapset in unique_maps_to_cache.values():
                beatmap_id = beatmap.get("id")
                if not beatmap_id:
                    continue

                map_data_from_db = db_get_map(beatmap_id, by="id")
                if not map_data_from_db or not map_data_from_db.get("md5_hash"):
                    continue

                hit_objects = (
                    beatmap.get("count_circles", 0)
                    + beatmap.get("count_sliders", 0)
                    + beatmap.get("count_spinners", 0)
                )

                update_data = {
                    "api_status": beatmap.get("status", "ranked"),
                    "artist": beatmapset.get("artist", ""),
                    "title": beatmapset.get("title", ""),
                    "version": beatmap.get("version", ""),
                    "creator": beatmapset.get("creator", ""),
                    "hit_objects": hit_objects,
                    "beatmapset_id": beatmapset.get("id"),
                }
                db_update_from_api(beatmap_id, update_data)

            summary_stats["precached_maps"] = len(unique_maps_to_cache)
            logger.info(f"Pre-caching complete for {len(unique_maps_to_cache)} maps")

    except requests.exceptions.RequestException as e:
        logger.exception("Could not pre-cache top scores data", e)

    report_progress("precache_top", 1, 1)

    announce_phase_start("pp_calc", phase_definitions, gui_log, phase_logger=logger)
    phase_key_pp = "pp_calc"
    base_pp, range_pp = progress_map.get(phase_key_pp, (current_progress_base, 0))
    summary_stats["replays_for_pp_calc"] = len(replays_for_pp_calc)
    logger.info(f"Processing {len(replays_for_pp_calc)} replays for PP calculation")

    score_list = []
    if replays_for_pp_calc:
        with ThreadPoolExecutor(max_workers=IO_THREAD_POOL_SIZE) as executor:
            futures = {
                executor.submit(
                    file_parser.process_osr_with_path, r_info[0], r_info[1]
                ): r_info[0]
                for r_info in replays_for_pp_calc
            }
            results = track_parallel_progress(
                futures,
                len(replays_for_pp_calc),
                progress_callback=lambda c, t: report_progress(phase_key_pp, c, t),
                gui_log=gui_log,
                progress_logger=logger,
                log_interval_sec=5,
                progress_message="Calculating PP",
                gui_update_step=1000,
            )
            score_list = [res for res in results if res is not None]

    else:
        logger.info("Skipping PP calculation: no replays found")

    summary_stats["calculated_scores"] = len(score_list)
    logger.info(f"PP calculation finished. Found {len(score_list)} valid scores")

    current_progress_base += range_pp

    announce_phase_start("find_lost", phase_definitions, gui_log, phase_logger=logger)

    lost, total_lost_count_pre_filter = find_lost_scores(score_list, CUTOFF_DATE)

    summary_stats["lost_scores_pre_filter"] = total_lost_count_pre_filter
    summary_stats["lost_scores_found"] = len(lost)

    logger.info(
        f"Filtered out {total_lost_count_pre_filter - len(lost)} scores. Final count: {len(lost)}"
    )

    announce_phase_start(
        "deferred_lookup", phase_definitions, gui_log, phase_logger=logger
    )
    md5s_to_lookup = {
        r["beatmap_md5"]
        for r in lost
        if not r.get("beatmap_id") and r.get("beatmap_md5")
    }
    run_deferred_lookup = bool(md5s_to_lookup)

    if run_deferred_lookup:
        base_deferred, range_deferred = progress_map.get(
            "deferred_lookup", (current_progress_base, 0)
        )
        total_to_lookup = len(md5s_to_lookup)
        summary_stats["maps_to_lookup_deferred"] = total_to_lookup
        logger.info(f"Performing deferred lookup for {total_to_lookup} maps...")

        last_log_time = time.time()
        for i, md5 in enumerate(md5s_to_lookup):
            report_progress("deferred_lookup", i + 1, total_to_lookup)
            progress_message = f"Looking up map details {i + 1}/{total_to_lookup}..."
            if gui_log:
                gui_log(progress_message, update_last=True)

            now = time.time()
            if logger and (now - last_log_time > 15 or (i + 1) == total_to_lookup):
                logger.info(progress_message)
                last_log_time = now

            lookup_result = osu_api_client.lookup_osu(md5)
            if lookup_result:
                pass

        logger.info("Deferred lookup phase finished")

        updated_lost = []
        for score in lost:
            md5 = score.get("beatmap_md5")
            if md5:
                fresh_map_data = db_get_map(md5, by="md5")
                if fresh_map_data:
                    updated_score = score.copy()
                    updated_score.update(fresh_map_data)
                    updated_lost.append(updated_score)
                else:
                    updated_lost.append(score)
            else:
                updated_lost.append(score)
        lost = updated_lost
        logger.info(
            f"Updated {len([s for s in lost if s.get('beatmap_id')])} lost scores with deferred lookup data"
        )

        current_progress_base += range_deferred
    else:
        logger.info("Skipping deferred lookup: no candidates found")
        report_progress("deferred_lookup", 1, 1)

    final_lost_list = []

    announce_phase_start(
        "validate_status", phase_definitions, gui_log, phase_logger=logger
    )
    ids_to_revalidate = []
    if not include_unranked:
        md5s_to_check = {rec["beatmap_md5"] for rec in lost if rec.get("beatmap_md5")}
        for md5 in md5s_to_check:
            map_data = db_get_map(md5, by="md5")
            if (
                map_data
                and map_data.get("beatmap_id")
                and map_data.get("api_status") in [None, "unknown"]
            ):
                ids_to_revalidate.append(map_data["beatmap_id"])
    run_validate_status = bool(ids_to_revalidate)

    if run_validate_status:
        base_validate, range_validate = progress_map.get(
            "validate_status", (current_progress_base, 0)
        )
        unique_ids = sorted(list(set(ids_to_revalidate)))
        summary_stats["maps_to_validate"] = len(unique_ids)
        logger.info(f"Validating map status for {len(unique_ids)} maps...")

        api_results = osu_api_client.maps_osu(
            unique_ids,
            gui_log=gui_log,
            logger=logger,
            progress_callback=lambda c, t: report_progress("validate_status", c, t),
        )

        for beatmap_id, beatmap_data in api_results.items():
            update_data = {
                "beatmapset_id": beatmap_data.get("beatmapset", {}).get("id"),
                "api_status": beatmap_data.get("status", "unknown"),
                "artist": beatmap_data.get("beatmapset", {}).get("artist"),
                "title": beatmap_data.get("beatmapset", {}).get("title"),
                "creator": beatmap_data.get("beatmapset", {}).get("creator"),
                "version": beatmap_data.get("version"),
            }
            db_update_from_api(beatmap_id, update_data)

        found_ids = set(api_results.keys())
        deleted_ids = [bid for bid in unique_ids if bid not in found_ids]
        for beatmap_id in deleted_ids:
            db_update_from_api(beatmap_id, {"api_status": "deleted"})

        summary_stats["maps_validated"] = len(found_ids)
        summary_stats["maps_deleted_on_validate"] = len(deleted_ids)
        logger.info(
            f"Status validation finished: {len(found_ids)} statuses updated, {len(deleted_ids)} maps not found (deleted)"
        )
        current_progress_base += range_validate
    else:
        reason = (
            "unranked maps included"
            if include_unranked
            else "no maps require validation"
        )
        logger.info(f"Skipping map status validation: {reason}")
        report_progress("validate_status", 1, 1)

    processed_md5s = set()
    for original_score in lost:
        md5 = original_score.get("beatmap_md5")
        if not md5 or md5 in processed_md5s:
            continue

        final_map_data = db_get_map(md5, by="md5")
        if not final_map_data:
            continue

        processed_md5s.add(md5)
        status = final_map_data.get("api_status")

        if include_unranked or (status in ["ranked", "approved"]):
            final_score_obj = original_score.copy()
            final_score_obj.update(final_map_data)
            final_lost_list.append(final_score_obj)

    logger.info(
        f"Filtered out {total_lost_count_pre_filter - len(final_lost_list)} scores. Final count: {len(final_lost_list)}"
    )
    summary_stats["lost_scores_found"] = len(final_lost_list)

    final_lost_count = len(final_lost_list)

    announce_phase_start("saving", phase_definitions, gui_log, phase_logger=logger)

    processed_lost_scores = []
    replay_manifest = []

    if final_lost_list:
        for rec in final_lost_list:
            rank_ = file_parser.grade_osu(
                rec.get("beatmap_id"),
                rec.get("count300", 0),
                rec.get("count50", 0),
                rec.get("countMiss", 0),
                rec.get("osu_file_path"),
            )

            processed_score = {
                "pp": rec["pp"],
                "beatmap_id": rec["beatmap_id"],
                "beatmap_md5": rec.get("beatmap_md5"),
                "artist": rec.get("artist", ""),
                "title": rec.get("title", ""),
                "creator": rec.get("creator", ""),
                "version": rec.get("version", ""),
                "beatmap": f"{rec.get('artist', '')} - {rec.get('title', '')} ({rec.get('creator', '')}) [{rec.get('version', '')}]",
                "mods": file_parser.sort_mods(rec["mods"]) if rec["mods"] else [],
                "count100": rec.get("count100", 0),
                "count50": rec.get("count50", 0),
                "countMiss": rec.get("countMiss", 0),
                "accuracy": rec["Accuracy"],
                "total_score": rec.get("total_score", ""),
                "score_time": rec.get("score_time", ""),
                "rank": rank_,
            }
            processed_lost_scores.append(processed_score)

            if rec.get("file_path"):
                replay_manifest.append(
                    {
                        "md5_hash": rec.get("beatmap_md5"),
                        "file_path": rec.get("file_path"),
                        "pp_claimed": rec["pp"],
                        "beatmap_id": rec["beatmap_id"],
                    }
                )

        if gui_log:
            gui_log("Lost scores data processed", update_last=True)
    else:
        logger.info("Empty list: no lost scores found")

    elapsed = time.time() - start_time
    summary_stats["total_time_seconds"] = int(elapsed)
    summary_stats["pre_filter_count"] = total_lost_count_pre_filter
    summary_stats["post_filter_count"] = final_lost_count

    metadata = {
        "total_time_seconds": int(elapsed),
        "user_identifier": user_identifier,
        "game_dir": game_dir,
    }

    logger.info("Full analysis finished in %.2f seconds", elapsed)

    return {
        "metadata": metadata,
        "summary_stats": summary_stats,
        "lost_scores": processed_lost_scores,
        "replay_manifest": replay_manifest,
    }


def make_top(
    game_dir,
    user_identifier,
    lookup_key,
    scan_results=None,
    gui_log=None,
    progress_callback=None,
    osu_api_client=None,
    include_unranked=False,
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
    if not scan_results or not scan_results.get("lost_scores"):
        error_message = "Scan results not provided or no lost scores found. Aborting potential top creation"
        logger.error(error_message)
        if gui_log:
            gui_log(error_message, update_last=False)
        return

    start = time.time()
    if gui_log:
        gui_log("Creating potential top...", update_last=False)
    db_init()
    if progress_callback:
        progress_callback(10, 100)

    try:
        user_json = osu_api_client.user_osu(user_identifier, lookup_key)
        if not user_json:
            if gui_log:
                gui_log(
                    f"Error: Failed to get user data '{user_identifier}' (type: {lookup_key})",
                    False,
                )
            raise ValueError(f"User not found: {user_identifier}")
    except OAuthSessionExpiredException:
        logger.warning(
            "OAuth session expired while getting user data for %s", user_identifier
        )
        raise

    username = user_json["username"]
    user_id = user_json["id"]
    if gui_log:
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
    top_data = parse_top(raw_top)
    top_data = calc_weight(top_data)
    total_weight_pp = sum(item["weight_PP"] for item in top_data)
    diff = overall_pp - total_weight_pp

    if gui_log:
        gui_log("Processing parsed top data...", update_last=False)
    if progress_callback:
        progress_callback(70, 100)

    parsed_top_processed = []
    for row in top_data:
        processed_row = {
            "pp": row["PP"],
            "beatmap_id": row["Beatmap ID"],
            "beatmap": row["Beatmap"],
            "mods": row["Mods"].split(", ")
            if row["Mods"] and row["Mods"] != "NM"
            else [],
            "count100": row["100"],
            "count50": row["50"],
            "countMiss": row["Misses"],
            "accuracy": row["Accuracy"],
            "score": row.get("Score", ""),
            "date": row.get("Score Date", ""),
            "weight_percent": row.get("weight_%", ""),
            "weight_pp": row.get("weight_PP", ""),
            "score_id": row["Score ID"],
            "rank": row["Rank"],
        }
        parsed_top_processed.append(processed_row)

    if gui_log:
        gui_log("Merging with lost scores...", update_last=False)
    if progress_callback:
        progress_callback(90, 100)

    lost_scores_data = scan_results["lost_scores"]

    lost_scores = []
    for score in lost_scores_data:
        lost_scores.append(
            {
                "PP": str(score["pp"]),
                "Beatmap ID": str(score["beatmap_id"]),
                "Beatmap MD5": score.get("beatmap_md5", ""),
                "Beatmap": score["beatmap"],
                "Mods": ", ".join(score["mods"]) if score["mods"] else "NM",
                "100": str(score["count100"]),
                "50": str(score["count50"]),
                "Misses": str(score["countMiss"]),
                "Accuracy": str(score["accuracy"]),
                "Score": str(score["total_score"]),
                "Date": score["score_time"],
                "Rank": score["rank"],
            }
        )

    for entry in top_data:
        try:
            bid = int(entry["Beatmap ID"])
            map_data = db_get_map(bid, by="id")
            if map_data:
                entry["artist"] = map_data.get("artist", "")
                entry["title"] = map_data.get("title", "")
                entry["creator"] = map_data.get("creator", "")
                entry["version"] = map_data.get("version", "")
                entry["Beatmap MD5"] = map_data.get("md5_hash", "")
            else:
                entry["artist"] = ""
                entry["title"] = entry.get("Beatmap", "Unknown")
                entry["creator"] = ""
                entry["version"] = ""
                entry["Beatmap MD5"] = ""
        except (KeyError, ValueError, TypeError):
            continue

    top_dict = {}
    for entry in top_data:
        try:
            bid = int(entry["Beatmap ID"])
        except (KeyError, ValueError, TypeError):
            continue
        if bid in top_dict:
            if entry["PP"] > top_dict[bid]["PP"]:
                top_dict[bid] = entry
        else:
            top_dict[bid] = entry

    original_lost_scores = scan_results.get("lost_scores", [])
    lost_by_id = {score.get("beatmap_id"): score for score in original_lost_scores}

    for lost in lost_scores:
        try:
            bid = int(lost["Beatmap ID"])
        except (KeyError, ValueError, TypeError):
            continue

        original_lost = lost_by_id.get(bid, {})

        lost_entry = {
            "PP": int(round(float(lost["PP"]))),
            "Beatmap ID": bid,
            "Beatmap MD5": lost.get("Beatmap MD5")
            or original_lost.get("beatmap_md5", ""),
            "Status": "lost",
            "Beatmap": lost["Beatmap"],
            "artist": original_lost.get("artist", ""),
            "title": original_lost.get("title", ""),
            "creator": original_lost.get("creator", ""),
            "version": original_lost.get("version", ""),
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

    top_with_lost_processed = []
    for row in top_with_lost:
        processed_row = {
            "pp": row["PP"],
            "beatmap_id": row["Beatmap ID"],
            "beatmap_md5": row.get("Beatmap MD5", ""),
            "status": row.get("Status", "ranked"),
            "beatmap": row["Beatmap"],
            "artist": row.get("artist", ""),
            "title": row.get("title", ""),
            "creator": row.get("creator", ""),
            "version": row.get("version", ""),
            "mods": row["Mods"].split(", ")
            if row["Mods"] and row["Mods"] != "NM"
            else [],
            "count100": row["100"],
            "count50": row["50"],
            "countMiss": row["Misses"],
            "accuracy": row["Accuracy"],
            "score": row.get("Score", ""),
            "date": row.get("Score Date", row.get("Date", "")),
            "rank": row["Rank"],
            "weight_percent": row.get("weight_%", ""),
            "weight_pp": row.get("weight_PP", ""),
            "score_id": row["Score ID"],
        }
        top_with_lost_processed.append(processed_row)

    lost_scores_count = len(lost_scores)
    lost_scores_avg_pp = 0
    avg_pp_lost_diff = 0
    diff_count = 0

    if lost_scores:
        total_pp = sum(int(round(float(s["PP"]))) for s in lost_scores)
        lost_scores_avg_pp = total_pp / lost_scores_count

        top_pp_by_map = {
            int(s["Beatmap ID"]): s["PP"]
            for s in top_data
            if "Beatmap ID" in s and "PP" in s
        }
        pp_diffs = []
        for lost_score in lost_scores:
            beatmap_id_raw = lost_score.get("Beatmap ID", 0)
            try:
                b_id = (
                    int(beatmap_id_raw)
                    if beatmap_id_raw and str(beatmap_id_raw).strip()
                    else 0
                )
            except (ValueError, TypeError):
                continue
            if b_id in top_pp_by_map:
                diff = float(lost_score["PP"]) - float(top_pp_by_map[b_id])
                if diff > 0:
                    pp_diffs.append(diff)

        if pp_diffs:
            avg_pp_lost_diff = sum(pp_diffs) / len(pp_diffs)
            diff_count = len(pp_diffs)

    extended_summary_stats = scan_results["summary_stats"].copy()
    extended_summary_stats.update(
        {
            "current_pp": overall_pp,
            "current_acc": overall_acc_from_api,
            "current_global_rank": user_json.get("statistics", {}).get(
                "global_rank", "N/A"
            ),
            "potential_pp": pot_pp,
            "potential_acc": overall_acc_lost,
            "delta_pp": diff_lost,
            "delta_acc": delta_acc,
            "weighted_pp_current": total_weight_pp,
            "weighted_pp_potential": total_weight_pp_new,
            "lost_scores_total": lost_scores_count,
            "lost_scores_avg_pp": lost_scores_avg_pp,
            "avg_pp_lost_diff": avg_pp_lost_diff,
            "avg_pp_lost_diff_count": diff_count,
        }
    )

    if gui_log:
        gui_log("Creating summary badge...", update_last=False)

    lost_ranked_count = extended_summary_stats.get("post_filter_count", 0)
    total_lost_count = extended_summary_stats.get("pre_filter_count", 0)

    badge_data = {
        "username": user_json.get("username"),
        "avatar_url": user_json.get("avatar_url"),
        "global_rank": extended_summary_stats["current_global_rank"],
        "current_pp": extended_summary_stats["current_pp"],
        "current_acc": extended_summary_stats["current_acc"],
        "potential_pp": extended_summary_stats["potential_pp"],
        "potential_acc": extended_summary_stats["potential_acc"],
        "delta_pp": extended_summary_stats["delta_pp"],
        "delta_acc": extended_summary_stats["delta_acc"],
        "lost_ranked_count": lost_ranked_count,
        "total_lost_count": total_lost_count,
        "scan_date": datetime.now().strftime("%d %b %Y"),
        "include_unranked": include_unranked,
    }

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    analysis_session_dir = os.path.join(ANALYSIS_DIR, timestamp)
    os.makedirs(analysis_session_dir, exist_ok=True)

    images_session_dir = os.path.join(IMAGES_DIR, timestamp)

    # noinspection PyBroadException
    try:
        os.makedirs(images_session_dir, exist_ok=True)

        badge_path = os.path.join(images_session_dir, "summary_badge.png")
        create_summary_badge(badge_data, badge_path, osu_api_client=osu_api_client)
        if gui_log:
            gui_log("Summary badge created successfully", update_last=False)
    except Exception as e:
        logger.exception("Failed to create summary badge: %s", e)
        if gui_log:
            gui_log(f"Error creating summary badge: {e}", update_last=False)

    elapsed = time.time() - start
    logger.info("Potential top created in %.2f sec", elapsed)
    if gui_log:
        gui_log(f"Potential top created in {elapsed:.2f} sec", update_last=False)
    if progress_callback:
        progress_callback(100, 100)

    metadata = scan_results["metadata"].copy()
    metadata.update(
        {
            "user_identifier": user_identifier,
            "game_dir": game_dir,
        }
    )

    complete_analysis = create_analysis_json_structure(
        metadata=metadata,
        summary_stats=extended_summary_stats,
        lost_scores=scan_results["lost_scores"],
        parsed_top=parsed_top_processed,
        top_with_lost=top_with_lost_processed,
        replay_manifest=scan_results.get("replay_manifest", []),
    )

    json_path = os.path.join(analysis_session_dir, "analysis_results.json")
    if save_analysis_to_json(complete_analysis, json_path):
        if gui_log:
            gui_log(f"Analysis results saved to {timestamp}/", update_last=False)
    else:
        if gui_log:
            gui_log("Failed to save analysis results", update_last=False)

    complete_analysis["session_dir"] = analysis_session_dir
    complete_analysis["images_dir"] = images_session_dir
    return complete_analysis
