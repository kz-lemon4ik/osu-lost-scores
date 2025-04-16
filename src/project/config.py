import os
import time
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

                                                    
dotenv_path = os.environ.get("DOTENV_PATH")

if not dotenv_path or not os.path.exists(dotenv_path):
                                                                                    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dotenv_path = os.path.join(project_root, "..", ".env")
    dotenv_path = os.path.abspath(dotenv_path)
    logger.warning(f"DOTENV_PATH не задан или файл не существует, используем: {dotenv_path}")

if os.path.exists(dotenv_path):
    logger.info(f"Загружаем .env из: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
else:
    logger.error(f"Не удалось найти .env файл: {dotenv_path}")

CLIENT_ID = os.environ.get("CLIENT_ID", "default_client_id")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "default_client_secret")
DB_FILE = os.environ.get("DB_FILE", "../cache/beatmap_info.db")

                                                          
if CLIENT_ID != "default_client_id":
    logger.info(f"CLIENT_ID загружен: {CLIENT_ID[:4]}...")
else:
    logger.warning("CLIENT_ID использует значение по умолчанию!")

                                              
cutoff_env = os.environ.get("CUTOFF_DATE", "1729728000")

                     
try:
    CUTOFF_DATE = int(cutoff_env)
except ValueError:
                                                                                
    logger.warning(f"Не удалось преобразовать CUTOFF_DATE '{cutoff_env}' в число, используем значение по умолчанию")
    CUTOFF_DATE = 1729728000                   