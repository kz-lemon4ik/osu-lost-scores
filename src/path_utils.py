
import os
import sys

def _bundle_root() -> str | None:
    
    return getattr(sys, "_MEIPASS", None)

def _exe_root() -> str | None:
    
    return os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else None

def get_project_root() -> str:
    
    return _exe_root() or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_env_path():
    
    return os.path.join(get_project_root(), ".env")

def get_standard_dir(dir_name):
    
    root_dir = get_project_root()
    return os.path.normpath(os.path.join(root_dir, dir_name))

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
    except (TypeError, AttributeError, ValueError):
        return path
