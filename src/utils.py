import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from PySide6.QtWidgets import QLineEdit, QMenu, QTextEdit

from color_constants import ImageColors
from path_utils import get_standard_dir

try:
    import pyperclip

    PYPERCLIP_AVAILABLE = True
except ImportError:
    pyperclip = None
    PYPERCLIP_AVAILABLE = False

logger = logging.getLogger(__name__)


def process_in_batches(
    items,
    batch_size=100,
    max_workers=None,
    process_func=None,
    progress_callback=None,
    gui_log=None,
    progress_logger=None,
    log_interval_sec=5,
    progress_message="Processing items",
    start_progress=0,
    progress_range=100,
):
    if not items:
        return []

    if max_workers is None:
        cpus = os.cpu_count() or 4
        max_workers = min(32, max(1, min(cpus * 2, len(items) // 10 + 1)))
    if batch_size is None:
        batch_size = max(50, min(1000, len(items) // 4))

    results = []
    total_items = len(items)
    processed_count = 0
    last_log_time = time.time()

    if process_func is None:
        raise ValueError("process_func cannot be None")

    for i in range(0, total_items, batch_size):
        batch = items[i : i + batch_size]

        if len(batch) <= 5:
            batch_results = [process_func(item) for item in batch]
            results.extend(batch_results)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results.extend(list(executor.map(process_func, batch)))

        processed_count += len(batch)

        gui_message = f"{progress_message} {processed_count}/{total_items}..."
        if gui_log:
            gui_log(gui_message, update_last=True)

        now = time.time()
        if progress_logger and (
            now - last_log_time > float(log_interval_sec)
            or processed_count == total_items
        ):
            progress_logger.info(gui_message)
            last_log_time = now

        if progress_callback:
            progress_value = (
                start_progress + (processed_count / total_items) * progress_range
            )
            progress_callback(int(progress_value), 100)

    return results


def track_parallel_progress(
    futures,
    total_items,
    progress_callback=None,
    gui_log=None,
    progress_logger=None,
    log_interval_sec=5,
    progress_message="Processing items",
    gui_update_step=1,
    start_progress=0,
    progress_range=100,
):
    results = []
    completed = 0
    last_log_time = time.time()

    for future in as_completed(futures):
        completed += 1
        try:
            result = future.result()
            results.append(result)
        except Exception as e:
            if progress_logger:
                progress_logger.error(
                    f"Error in parallel task for '{progress_message}': {e}"
                )

        if gui_log and (completed % gui_update_step == 0 or completed == total_items):
            gui_message = f"{progress_message} {completed}/{total_items}..."
            gui_log(gui_message, update_last=True)

        now = time.time()
        if progress_logger and (
            now - last_log_time > float(log_interval_sec) or completed == total_items
        ):
            log_message = f"{progress_message} {completed}/{total_items}..."
            progress_logger.info(log_message)
            last_log_time = now

        if progress_callback:
            progress_value = start_progress + (completed / total_items) * progress_range
            progress_callback(int(progress_value), 100)

    return results


def load_summary_stats():
    latest_session = find_latest_analysis_session()
    if latest_session:
        json_path = os.path.join(latest_session, "analysis_results.json")
        try:
            json_data = load_analysis_from_json(json_path)
            if json_data:
                return json_data.get("summary_stats", {})
        except Exception as e:
            logger.exception(
                "Error loading summary stats from JSON %s: %s", json_path, e
            )

    return None


def create_standard_edit_menu(widget):
    menu = QMenu()
    if not isinstance(widget, (QLineEdit, QTextEdit)):
        return menu

    cut_action = menu.addAction("Cut")
    cut_action.triggered.connect(widget.cut)
    if isinstance(widget, QLineEdit):
        cut_action.setEnabled(widget.hasSelectedText())
    else:
        cut_action.setEnabled(bool(widget.textCursor().selectedText()))

    copy_action = menu.addAction("Copy")
    copy_action.triggered.connect(widget.copy)
    if isinstance(widget, QLineEdit):
        copy_action.setEnabled(widget.hasSelectedText())
    else:
        copy_action.setEnabled(bool(widget.textCursor().selectedText()))

    paste_action = menu.addAction("Paste")
    paste_action.triggered.connect(widget.paste)
    if PYPERCLIP_AVAILABLE and pyperclip and pyperclip.paste():
        paste_action.setEnabled(True)
    elif not PYPERCLIP_AVAILABLE:
        paste_action.setEnabled(True)
    else:
        paste_action.setEnabled(False)

    menu.addSeparator()

    select_all_action = menu.addAction("Select All")
    select_all_action.triggered.connect(widget.selectAll)

    text_content = ""
    if isinstance(widget, QLineEdit):
        text_content = widget.text()
    elif isinstance(widget, QTextEdit):
        text_content = widget.toPlainText()
    select_all_action.setEnabled(bool(text_content))

    return menu


def get_delta_color(value):
    if value > 0:
        return ImageColors.GREEN
    if value < 0:
        return ImageColors.RED
    return ImageColors.WHITE


class RateLimiter:
    def __init__(self, requests_per_minute):
        self._lock = threading.Lock()
        self._last_call_time = 0
        if requests_per_minute <= 0:
            self.delay_seconds = 0
        else:
            self.delay_seconds = 60.0 / requests_per_minute

    def wait(self):
        if self.delay_seconds == 0:
            return

        with self._lock:
            now = time.time()
            time_since_last_call = now - self._last_call_time
            if time_since_last_call < self.delay_seconds:
                sleep_time = self.delay_seconds - time_since_last_call
                time.sleep(sleep_time)
            self._last_call_time = time.time()


def save_analysis_to_json(analysis_data, filepath):
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(analysis_data, f, ensure_ascii=False, indent=2)

        logger.info("Analysis results saved to %s", filepath)
        return True

    except Exception as e:
        logger.exception("Failed to save analysis to JSON: %s", e)
        return False


def load_analysis_from_json(filepath):
    try:
        if not os.path.exists(filepath):
            logger.warning("Analysis JSON file not found: %s", filepath)
            return None

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info("Analysis results loaded from %s", filepath)
        return data

    except Exception as e:
        logger.exception("Failed to load analysis from JSON: %s", e)
        return None


def create_analysis_json_structure(
    metadata,
    summary_stats,
    lost_scores,
    parsed_top,
    top_with_lost,
    replay_manifest=None,
):
    return {
        "metadata": {
            "analysis_timestamp": datetime.now().isoformat(),
            "total_time_seconds": metadata.get("total_time_seconds", 0),
            "user_identifier": metadata.get("user_identifier", ""),
            "game_dir": metadata.get("game_dir", ""),
            "client_version": metadata.get("client_version", "1.0.0"),
        },
        "summary_stats": summary_stats or {},
        "lost_scores": lost_scores or [],
        "parsed_top": parsed_top or [],
        "top_with_lost": top_with_lost or [],
        "replay_manifest": replay_manifest or [],
        "signature": {"hmac": None, "timestamp": None},
    }


def load_summary_stats_from_json(json_data):
    if not json_data:
        return None

    return json_data.get("summary_stats", {})


def find_latest_analysis_session():
    try:
        analysis_dir = get_standard_dir("data/analysis")
        if not os.path.exists(analysis_dir):
            return None

        sessions = []
        for item in os.listdir(analysis_dir):
            item_path = os.path.join(analysis_dir, item)
            if os.path.isdir(item_path) and len(item) == 19:  # YYYY-MM-DD_HH-MM-SS
                try:
                    datetime.strptime(item, "%Y-%m-%d_%H-%M-%S")
                    sessions.append(item)
                except ValueError:
                    continue

        if not sessions:
            return None

        sessions.sort(reverse=True)
        latest_session = sessions[0]

        return os.path.join(analysis_dir, latest_session)

    except Exception as e:
        logger.exception("Error finding latest analysis session: %s", e)
        return None


def find_latest_images_session():
    try:
        images_dir = get_standard_dir("data/images")
        if not os.path.exists(images_dir):
            return None

        sessions = []
        for item in os.listdir(images_dir):
            item_path = os.path.join(images_dir, item)
            if os.path.isdir(item_path) and len(item) == 19:  # YYYY-MM-DD_HH-MM-SS
                try:
                    datetime.strptime(item, "%Y-%m-%d_%H-%M-%S")
                    sessions.append(item)
                except ValueError:
                    continue

        if not sessions:
            return None

        sessions.sort(reverse=True)
        latest_session = sessions[0]

        return os.path.join(images_dir, latest_session)

    except Exception as e:
        logger.exception("Error finding latest images session: %s", e)
        return None
