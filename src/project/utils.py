import os
import sys


def get_resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        base_dir = sys._MEIPASS
    else:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    return os.path.normpath(os.path.join(base_dir, relative_path))