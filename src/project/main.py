import logging
                        
from config import CLIENT_ID, CLIENT_SECRET
from gui import create_gui

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
                                                           
    create_gui()

if __name__ == "__main__":
    main()
