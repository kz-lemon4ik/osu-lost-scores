import logging
                        
from config import CLIENT_ID, CLIENT_SECRET
from gui import create_gui

LOG_FILENAME = "log.txt"                                 
log_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)-5.5s] [%(name)-15.15s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"                      
)
root_logger = logging.getLogger()                           
root_logger.setLevel(logging.INFO)                                         

                                                                      
                                                                  
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

                      
try:
    file_handler = logging.FileHandler(LOG_FILENAME, encoding='utf-8', mode='w')                                                                 
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
except Exception as e:
    print(f"Не удалось настроить логирование в файл {LOG_FILENAME}: {e}")


                        
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

logging.info("Логирование настроено. Вывод в консоль и файл %s", LOG_FILENAME)
                                           

def main():
    create_gui()

if __name__ == "__main__":
    main()
