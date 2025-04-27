import os
import sys


def get_resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        base_dir = sys._MEIPASS
    else:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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