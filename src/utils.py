
import csv
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        batch = items[i: i + batch_size]

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
                now - last_log_time > float(log_interval_sec) or processed_count == total_items
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
    
    summary_path = os.path.join(get_standard_dir("csv"), "lost_scores_summary.csv")
    if not os.path.exists(summary_path):
        return None

    stats = {}
    try:
        with open(summary_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            if next(reader, None) is None:
                return None

            for row in reader:
                if len(row) == 2:
                    stats[row[0]] = row[1]
        return stats if stats else None
    except Exception as e:
        logger.exception("Error loading summary stats from %s: %s", summary_path, e)
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
