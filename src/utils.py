import os
import sys


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
        dirname, filename = os.path.split(path)
        parent = os.path.basename(dirname)
        return os.path.join(os.sep, parent, filename)
    except Exception:
        return path
