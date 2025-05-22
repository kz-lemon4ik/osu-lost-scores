import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


def get_app_dir():
    if hasattr(sys, "_MEIPASS"):
        return sys._MEIPASS
    else:
        return os.path.dirname(os.path.abspath(__file__))


def get_project_root():
    if hasattr(sys, "_MEIPASS"):
        return sys._MEIPASS
    else:
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_env_path():
    return os.path.join(get_project_root(), ".env")


def get_standard_dir(dir_name):
    root_dir = get_project_root()
    return os.path.normpath(os.path.join(root_dir, dir_name))


def get_resource_path(relative_path):
    root_dir = get_project_root()

    return os.path.normpath(os.path.join(root_dir, relative_path))


def ensure_app_dirs_exist():
    standard_dirs = ["cache", "results", "maps", "csv", "config", "log"]
    for dir_name in standard_dirs:
        dir_path = get_standard_dir(dir_name)
        os.makedirs(dir_path, exist_ok=True)


def mask_path_for_log(path):
    if not path:
        return path
    try:
                                                           
        path = path.replace("\\", "/") if isinstance(path, str) else path

                                                                           
        base_dirs = ["cache", "results", "maps", "csv", "log"]
        project_root = get_project_root().replace("\\", "/")

                                                                        
        for base_name in base_dirs:
            base_dir = f"{project_root}/{base_name}"
            if isinstance(path, str) and base_dir in path:
                rel_path = path.split(base_dir)[-1].lstrip("/")
                return f"{base_name}/{rel_path}"

                                                                           
        dirname, filename = os.path.split(path)
        parent = os.path.basename(dirname)
        return f"{parent}/{filename}"
    except Exception:
        return path


def process_in_batches(
    items,
    batch_size=100,
    max_workers=None,
    process_func=None,
    progress_callback=None,
    gui_log=None,
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

                                                              
    last_update_time = time.time()
    update_frequency = max(1, min(total_items // 20, total_items // 100 * 5))

    for i in range(0, total_items, batch_size):
        batch = items[i : i + batch_size]
        batch_results = []

                                                           
        if len(batch) <= 5:
            batch_results = [process_func(item) for item in batch]
            processed_count += len(batch)

            if (
                processed_count % update_frequency == 0
                or processed_count == total_items
                or time.time() - last_update_time >= 1.0
            ):
                last_update_time = time.time()
                progress_value = (
                    start_progress + (processed_count / total_items) * progress_range
                )
                if progress_callback:
                    progress_callback(int(progress_value), 100)
                if gui_log:
                    gui_log(
                        f"Processing items {processed_count}/{total_items}",
                        update_last=True,
                    )
        else:
                                                       
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                                                                                        
                batch_futures = list(executor.map(process_func, batch))
                batch_results = batch_futures
                processed_count += len(batch)

                if (
                    processed_count % update_frequency == 0
                    or processed_count == total_items
                    or time.time() - last_update_time >= 1.0
                ):
                    last_update_time = time.time()
                    progress_value = (
                        start_progress
                        + (processed_count / total_items) * progress_range
                    )
                    if progress_callback:
                        progress_callback(int(progress_value), 100)
                    if gui_log:
                        gui_log(
                            f"Processing items {processed_count}/{total_items}",
                            update_last=True,
                        )

        results.extend(batch_results)

    return results


def track_parallel_progress(
    futures,
    total_items,
    progress_callback=None,
    gui_log=None,
    progress_message="Processing items",
    start_progress=0,
    progress_range=100,
    update_every=None,
):
    results = []
    completed = 0

                                                  
    if update_every is not None:
        update_frequency = update_every
    else:
        update_frequency = max(1, min(total_items // 20, total_items // 100 * 5))

    for future in as_completed(futures):
        completed += 1
        try:
            result = future.result()
            results.append(result)
        except Exception as e:
            logger.error(f"Error in parallel task: {e}")

        if completed % update_frequency == 0 or completed == total_items:
            progress_value = start_progress + (completed / total_items) * progress_range
            if progress_callback:
                progress_callback(int(progress_value), 100)
            if gui_log:
                gui_log(
                    f"{progress_message} {completed}/{total_items}", update_last=True
                )

    return results
