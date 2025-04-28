import os
import sys


def get_resource_path(relative_path):
    if hasattr(sys, "_MEIPASS"):
        base_dir = sys._MEIPASS
    else:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

                                            
    if relative_path.startswith("cache/"):
        from config import CACHE_DIR
        return os.path.normpath(os.path.join(CACHE_DIR, relative_path[6:]))
    elif relative_path.startswith("results/"):
        from config import RESULTS_DIR
        return os.path.normpath(os.path.join(RESULTS_DIR, relative_path[8:]))
    elif relative_path.startswith("maps/"):
        from config import MAPS_DIR
        return os.path.normpath(os.path.join(MAPS_DIR, relative_path[5:]))
    elif relative_path.startswith("csv/"):
        from config import CSV_DIR
        return os.path.normpath(os.path.join(CSV_DIR, relative_path[4:]))

    return os.path.normpath(os.path.join(base_dir, relative_path))

def mask_path_for_log(path):
                                                                                                      
    if not path:
        return path
    try:
        dirname, filename = os.path.split(path)
        parent = os.path.basename(dirname)
        return os.path.join(os.sep, parent, filename)
    except Exception:
        return path
