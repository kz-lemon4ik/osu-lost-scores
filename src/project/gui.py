import sys
import os
import platform
import threading
import logging
import time
import json
from functools import partial
from datetime import datetime

from PySide6 import QtWidgets, QtCore, QtGui
from PySide6.QtCore import Qt, Signal, QRunnable, QThreadPool, QObject, Slot, QPropertyAnimation, QEasingCurve
from PySide6.QtGui import QPixmap, QPainter, QFontDatabase, QAction, QIcon, QFont, QColor
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QProgressBar, QTextEdit, QFileDialog, QMessageBox, QMenu, QFrame
)

try:
    import pyperclip

    PYPERCLIP_AVAILABLE = True
except ImportError:
    print(
        "ПРЕДУПРЕЖДЕНИЕ: pyperclip не найден (pip install pyperclip). Копирование/вставка могут работать некорректно.")
    PYPERCLIP_AVAILABLE = False

import generate_image as img_mod
from analyzer import scan_replays, make_top

BASE_SRC_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ICON_PATH = os.path.join(BASE_SRC_PATH, "assets", "icons")
FONT_PATH = os.path.join(BASE_SRC_PATH, "assets", "fonts")
BACKGROUND_FOLDER_PATH = os.path.join(BASE_SRC_PATH, "assets", "background")
BACKGROUND_IMAGE_PATH = os.path.join(BACKGROUND_FOLDER_PATH, "bg.png")
CONFIG_PATH = os.path.join(BASE_SRC_PATH, "config", "gui_config.json")

                                                        
os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)

BG_COLOR = "#251a37"
FG_COLOR = "#302444"
ACCENT_COLOR = "#ee4bbd"
NORMAL_BORDER_COLOR = "#4A3F5F"
SUBTLE_BORDER_COLOR = FG_COLOR
TEXT_COLOR = "#FFFFFF"
PLACEHOLDER_COLOR = "#A0A0A0"

BUTTON_HOVER_STYLE = f"QPushButton {{ background-color: {FG_COLOR}; border: 1px solid {ACCENT_COLOR}; border-radius: 5px; }}"
BUTTON_NORMAL_STYLE = ""


class WorkerSignals(QObject):
    progress = Signal(int, int)
    log = Signal(str, bool)
    finished = Signal()
    error = Signal(str)


class Worker(QRunnable):
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()
        if 'progress_callback' in self.fn.__code__.co_varnames:
            self.kwargs['progress_callback'] = partial(self.emit_progress)
        if 'gui_log' in self.fn.__code__.co_varnames:
            self.kwargs['gui_log'] = partial(self.emit_log)

    @Slot()
    def run(self):
        try:
            self.fn(*self.args, **self.kwargs)
        except Exception as e:
            self.signals.error.emit(str(e))
        finally:
            self.signals.finished.emit()

    def emit_progress(self, current, total):
        self.signals.progress.emit(current, total)

    def emit_log(self, message, update_last=False):
        self.signals.log.emit(message, update_last)


class HoverButton(QPushButton):
    def __init__(self, text, normal_icon=None, hover_icon=None, parent=None):
        super().__init__(text, parent)
        self.normal_icon = normal_icon if normal_icon else QIcon()
        self.hover_icon = hover_icon if hover_icon else QIcon()
        self.setIcon(self.normal_icon)
        self.setMouseTracking(True)

    def enterEvent(self, event):
        if self.objectName() != "BrowseButton":
            self.setStyleSheet(BUTTON_HOVER_STYLE)
        if self.hover_icon and not self.hover_icon.isNull():
            self.setIcon(self.hover_icon)
        super().enterEvent(event)

    def leaveEvent(self, event):
        if self.objectName() != "BrowseButton":
            self.setStyleSheet(BUTTON_NORMAL_STYLE)
        if self.normal_icon and not self.normal_icon.isNull():
            self.setIcon(self.normal_icon)
        super().leaveEvent(event)


class FolderButton(QPushButton):
    def __init__(self, normal_icon=None, hover_icon=None, parent=None):
        super().__init__(parent)
        self.normal_icon = normal_icon if normal_icon else QIcon()
        self.hover_icon = hover_icon if hover_icon else QIcon()
        self.setIcon(self.normal_icon)
        self.setMouseTracking(True)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setStyleSheet("QPushButton { background: transparent; border: none; }")

    def enterEvent(self, event):
        if self.hover_icon and not self.hover_icon.isNull():
            self.setIcon(self.hover_icon)
        super().enterEvent(event)

    def leaveEvent(self, event):
        if self.normal_icon and not self.normal_icon.isNull():
            self.setIcon(self.normal_icon)
        super().leaveEvent(event)


class AnimatedProgressBar(QProgressBar):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTextVisible(False)

                                            
        self.animation = QPropertyAnimation(self, b"value")
        self.animation.setEasingCurve(QEasingCurve.Type.OutCubic)
        self.animation.setDuration(500)                              

    def setValue(self, value):
                                                                
        self.animation.stop()

                                                            
        self.animation.setStartValue(self.value())
        self.animation.setEndValue(value)

                            
        self.animation.start()


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("osu! Lost Scores Analyzer")
        self.setGeometry(100, 100, 650, 500)
        self.setFixedSize(650, 500)

                                             
        self.scan_completed = threading.Event()
        self.top_completed = threading.Event()
        self.img_completed = threading.Event()

                                                          
        self.overall_progress = 0
                                      
        self.current_task = "Готово к запуску"

        self.load_fonts()
        self.load_icons()
        self.load_background()
        self.load_config()
        self.initUI()
        self.threadpool = QThreadPool()
        print(f"Макс. потоков в пуле: {self.threadpool.maxThreadCount()}")
        self._try_auto_detect_osu_path()

    def ensure_csv_files_exist(self):
                                                                 
        csv_dir = os.path.join(os.path.dirname(__file__), "..", "csv")
        os.makedirs(csv_dir, exist_ok=True)

                                                             
                         
        lost_scores_path = os.path.join(csv_dir, "lost_scores.csv")
        if not os.path.exists(lost_scores_path):
            try:
                with open(lost_scores_path, "w", encoding="utf-8") as f:
                    f.write("PP,Beatmap ID,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,Rank\n")
                self.append_log("Создан пустой файл lost_scores.csv", False)
            except Exception as e:
                self.append_log(f"Ошибка при создании lost_scores.csv: {e}", False)

                        
        parsed_top_path = os.path.join(csv_dir, "parsed_top.csv")
        if not os.path.exists(parsed_top_path):
            try:
                with open(parsed_top_path, "w", encoding="utf-8") as f:
                    f.write(
                        "PP,Beatmap ID,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,weight_%,weight_PP,Score ID,Rank\n")
                self.append_log("Создан пустой файл parsed_top.csv", False)
            except Exception as e:
                self.append_log(f"Ошибка при создании parsed_top.csv: {e}", False)

                           
        top_with_lost_path = os.path.join(csv_dir, "top_with_lost.csv")
        if not os.path.exists(top_with_lost_path):
            try:
                with open(top_with_lost_path, "w", encoding="utf-8") as f:
                    f.write(
                        "PP,Beatmap ID,Status,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,Rank,weight_%,weight_PP,Score ID\n")
                self.append_log("Создан пустой файл top_with_lost.csv", False)
            except Exception as e:
                self.append_log(f"Ошибка при создании top_with_lost.csv: {e}", False)

    def load_config(self):
                                               
        self.config = {}
        try:
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
                print(f"Конфигурация загружена из {CONFIG_PATH}")
        except Exception as e:
            print(f"Ошибка загрузки конфигурации: {e}")
            self.config = {}

    def save_config(self):
                                             
        try:
                                                      
            self.config['osu_path'] = self.game_entry.text().strip()

            with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=4)

            print(f"Конфигурация сохранена в {CONFIG_PATH}")
        except Exception as e:
            print(f"Ошибка сохранения конфигурации: {e}")

    def load_fonts(self):

        font_db = QFontDatabase()
        fonts_loaded = 0
        if os.path.isdir(FONT_PATH):
            for filename in os.listdir(FONT_PATH):
                if filename.lower().endswith((".ttf", ".otf")):
                    font_id = font_db.addApplicationFont(os.path.join(FONT_PATH, filename))
                    if font_id != -1:
                        fonts_loaded += 1
                    else:
                        print(f" -> Ошибка загрузки шрифта: {filename}")
            if fonts_loaded > 0:
                print(f"Загружено {fonts_loaded} локальных шрифтов.")
            else:
                print(f"Локальные шрифты в {FONT_PATH} не загружены.")
        else:
            print(f"Папка со шрифтами не найдена: {FONT_PATH}")

        self.title_font = QFont("Exo 2", 24, QFont.Weight.Bold)
        self.button_font = QFont("Exo 2", 14, QFont.Weight.Bold)
        self.label_font = QFont("Exo 2", 14)
        self.entry_font = QFont("Exo 2", 10, weight=QFont.Weight.Normal, italic=True)
        self.log_font = QFont("Exo 2", 10)
        self.log_font.setItalic(True)

    def load_icons(self):

        self.icons = {}
        icon_files_qt = {
            "scan": {"normal": "scan_normal.png", "hover": "scan_hover.png"},
            "trophy": {"normal": "trophy_normal.png", "hover": "trophy_hover.png"},
            "image": {"normal": "image_icon_normal.png", "hover": "image_icon_hover.png"},
            "folder": {"normal": "folder_normal.png", "hover": "folder_hover.png"}
        }
        for name, states in icon_files_qt.items():
            self.icons[name] = {}
            for state, filename in states.items():
                path = os.path.join(ICON_PATH, filename)
                if os.path.exists(path):
                    self.icons[name][state] = QIcon(path)
                else:
                    print(f"Файл иконки не найден: {path}")
                    if state == 'hover' and 'normal' in self.icons.get(name, {}):
                        self.icons[name][state] = self.icons[name]['normal']
                    else:
                        self.icons[name][state] = QIcon()

    def load_background(self):

        self.background_pixmap = None
        if os.path.exists(BACKGROUND_IMAGE_PATH):
            try:
                self.background_pixmap = QPixmap(BACKGROUND_IMAGE_PATH)
                if self.background_pixmap.isNull():
                    self.background_pixmap = None
                    print(f"Не удалось загрузить фон: {BACKGROUND_IMAGE_PATH}")
                else:
                    print("Фоновое изображение загружено.")
            except Exception as e:
                print(f"Ошибка загрузки фона: {e}")
                self.background_pixmap = None
        else:
            print(f"Файл фона не найден: {BACKGROUND_IMAGE_PATH}")

    def paintEvent(self, event):

        painter = QPainter(self)
        if self.background_pixmap:
            scaled_pixmap = self.background_pixmap.scaled(self.size(), Qt.AspectRatioMode.IgnoreAspectRatio,
                                                          Qt.TransformationMode.SmoothTransformation)
            painter.drawPixmap(self.rect(), scaled_pixmap)
        else:
            painter.fillRect(self.rect(), QColor(BG_COLOR))
        painter.end()

    def initUI(self):
                                
        window_height = 650                           
        self.setGeometry(100, 100, 650, window_height)
        self.setFixedSize(650, window_height)

                                                
        self.setLayout(None)                    

                                                     
        self.title_label = QLabel(self)
        self.title_label.setGeometry(50, 20, 550, 50)
        self.title_label.setObjectName("TitleLabel")
        self.title_label.setFont(self.title_font)
        self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.title_label.setText(
            '<span style="color: #ee4bbd;">osu!</span><span style="color: white;"> Lost Scores Analyzer</span> 🍋')
        self.title_label.setTextFormat(Qt.TextFormat.RichText)

                           
        dir_label = QLabel("osu! Game Directory", self)
        dir_label.setGeometry(50, 90, 550, 30)
        dir_label.setFont(self.label_font)

                                                         
        dir_container = QFrame(self)
        dir_container.setGeometry(50, 125, 550, 40)
        dir_container.setStyleSheet(f"""
            QFrame {{
                background-color: {FG_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 5px;
            }}
            QFrame:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)

                                                 
        self.game_entry = QLineEdit(dir_container)
        self.game_entry.setGeometry(10, 0, 500, 40)
        self.game_entry.setFont(self.entry_font)
        self.game_entry.setPlaceholderText("Path to your osu! installation folder...")
        self.game_entry.setStyleSheet("""
            QLineEdit {
                background-color: transparent;
                border: none;
                padding: 5px;
            }
        """)

                                                                            
        self.browse_button = FolderButton(self.icons.get("folder", {}).get("normal"),
                                          self.icons.get("folder", {}).get("hover"), dir_container)
                                        
        self.browse_button.setGeometry(510, 5, 30, 30)
        self.browse_button.clicked.connect(self.browse_directory)

                        
        url_label = QLabel("Nickname (or ID / URL)", self)
        url_label.setGeometry(50, 180, 550, 30)
        url_label.setFont(self.label_font)

        self.profile_entry = QLineEdit(self)
        self.profile_entry.setGeometry(50, 215, 550, 40)
        self.profile_entry.setFont(self.entry_font)
        self.profile_entry.setPlaceholderText("e.g., https://osu.ppy.sh/users/2")
        self.profile_entry.setStyleSheet(f"""
            QLineEdit {{
                background-color: {FG_COLOR};
                color: {TEXT_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 5px;
                padding: 5px;
            }}
            QLineEdit:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)

                                  
        scores_label = QLabel("Number of scores to display", self)
        scores_label.setGeometry(50, 270, 550, 30)
        scores_label.setFont(self.label_font)

        self.scores_count_entry = QLineEdit(self)
        self.scores_count_entry.setGeometry(50, 305, 550, 40)
        self.scores_count_entry.setFont(self.entry_font)
        self.scores_count_entry.setPlaceholderText("For example, 10")
        self.scores_count_entry.setStyleSheet(f"""
            QLineEdit {{
                background-color: {FG_COLOR};
                color: {TEXT_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 5px;
                padding: 5px;
            }}
            QLineEdit:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)

        validator = QtGui.QIntValidator(1, 100, self)
        self.scores_count_entry.setValidator(validator)

                                                      
        self.btn_scan = QPushButton(self)
        self.btn_scan.setGeometry(0, 0, 0, 0)                                    
        self.btn_scan.clicked.connect(self.start_scan)

        self.btn_top = QPushButton(self)
        self.btn_top.setGeometry(0, 0, 0, 0)                   
        self.btn_top.clicked.connect(self.start_top)

        self.btn_img = QPushButton(self)
        self.btn_img.setGeometry(0, 0, 0, 0)                   
        self.btn_img.clicked.connect(self.start_img)

                                                                        
        btn_all_width = 550
        btn_y = 370
        self.btn_all = HoverButton("Start Scan", None, None, self)
        self.btn_all.setGeometry(50, btn_y, btn_all_width, 50)
        self.btn_all.setFont(self.button_font)
                                       
        self.btn_all.setStyleSheet(f"""
            QPushButton {{
                background-color: {FG_COLOR};
                color: {TEXT_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 5px;
                text-align: center;
            }}
            QPushButton:hover {{
                border: 2px solid {ACCENT_COLOR};
                background-color: {FG_COLOR};
            }}
        """)
        self.btn_all.clicked.connect(self.start_all_processes)

                                  
        self.progress_bar = AnimatedProgressBar(self)
        self.progress_bar.setGeometry(50, 440, 550, 20)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setStyleSheet(f"""
            QProgressBar {{
                background-color: {FG_COLOR}; 
                color: {TEXT_COLOR};
                border: none;
                border-radius: 8px;
                text-align: center;
            }}
            QProgressBar::chunk {{
                background-color: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 {ACCENT_COLOR}, stop:1 #9932CC);
                border-radius: 7px;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 {ACCENT_COLOR}, stop:1 #9932CC);
            }}
        """)

        self.status_label = QLabel(self.current_task, self)
        self.status_label.setGeometry(50, 465, 550, 25)
        self.status_label.setObjectName("StatusLabel")
        status_font = QFont("Exo 2", 11)
        status_font.setItalic(True)
        self.status_label.setFont(status_font)
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_label.setStyleSheet(f"QLabel#StatusLabel {{ color: {TEXT_COLOR}; background-color: transparent; }}")

             
        log_label = QLabel("Log", self)
        log_label.setGeometry(50, 500, 550, 25)
        log_label.setFont(self.label_font)

        log_container = QFrame(self)
        log_container.setGeometry(50, 530, 550, 100)                                      
        log_container.setObjectName("LogContainer")
        log_container.setFrameShape(QFrame.Shape.NoFrame)
        log_container.setAutoFillBackground(True)
        log_container.setStyleSheet(f"""
            QFrame#LogContainer {{
                background-color: {FG_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 5px;
            }}
            QFrame#LogContainer:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)

                                                        
        log_layout = QVBoxLayout(log_container)
        log_layout.setContentsMargins(5, 5, 5, 5)

        self.log_textbox = QTextEdit(log_container)
        self.log_textbox.setFont(self.log_font)
        self.log_textbox.setReadOnly(True)
        self.log_textbox.setStyleSheet(f"""
            QTextEdit {{ 
                background-color: {FG_COLOR}; 
                color: {TEXT_COLOR};
                border: none; 
            }}
        """)
        log_layout.addWidget(self.log_textbox)

                          
        self.log_textbox.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.log_textbox.customContextMenuRequested.connect(partial(self.show_context_menu, self.log_textbox))

        self.game_entry.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.game_entry.customContextMenuRequested.connect(partial(self.show_context_menu, self.game_entry))

        self.profile_entry.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.profile_entry.customContextMenuRequested.connect(partial(self.show_context_menu, self.profile_entry))

        self.scores_count_entry.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.scores_count_entry.customContextMenuRequested.connect(
            partial(self.show_context_menu, self.scores_count_entry))

        self.setStyleSheet(self.get_stylesheet())

    def get_stylesheet(self):
        return f"""
            QWidget {{ background-color: transparent; color: {TEXT_COLOR}; }}
            QLabel {{ background-color: transparent; color: {TEXT_COLOR}; }}
            QLabel#TitleLabel {{ background-color: transparent; }}

            /* Стили для скроллбара */
            QScrollBar:vertical {{ 
                border: none; 
                background: {FG_COLOR}; 
                width: 8px; 
                margin: 0; 
            }}
            QScrollBar::handle:vertical {{ 
                background: {NORMAL_BORDER_COLOR}; 
                min-height: 20px; 
                border-radius: 4px; 
            }}
            QScrollBar::handle:vertical:hover {{ 
                background: {ACCENT_COLOR}; 
            }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ 
                border: none; 
                background: none; 
                height: 0px; 
            }}
            QScrollBar::up-arrow:vertical, QScrollBar::down-arrow:vertical {{ 
                background: none; 
            }}
            QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{ 
                background: none; 
            }}
        """

    @Slot(str, bool)
    def append_log(self, message, update_last):

        try:
            if update_last:
                                                   
                self.current_task = message
                self.status_label.setText(message)

            else:
                                                                                           
                if message:
                    self.current_task = message
                    self.status_label.setText(message)

                cursor = self.log_textbox.textCursor()
                cursor.movePosition(QtGui.QTextCursor.MoveOperation.End)
                stamp = datetime.now().strftime("[%H:%M:%S] ")
                full_gui_message = stamp + message + "\n"
                cursor.insertText(full_gui_message)
                self.log_textbox.ensureCursorVisible()

            gui_file_logger = logging.getLogger("gui")
            gui_file_logger.info(message)

        except Exception as e:
            error_logger = logging.getLogger("gui_error")
            error_logger.exception(f"Исключение внутри append_log при обработке сообщения '{message}': {e}")

    @Slot(int, int)
    def update_progress_bar(self, current, total):
                                                     
        if self.scan_completed.is_set() and not self.top_completed.is_set():
                                                           
            progress = 30 + int((current / total) * 30) if total > 0 else 30
        elif self.scan_completed.is_set() and self.top_completed.is_set():
                                                     
            progress = 60 + int((current / total) * 40) if total > 0 else 60
        else:
                                                  
            progress = int((current / total) * 30) if total > 0 else 0

                                                           
        self.overall_progress = progress
        self.progress_bar.setValue(progress)

    @Slot()
    def task_finished(self):
        print("Фоновая задача завершена.")
                                                                 
        if not self.scan_completed.is_set():
            self.progress_bar.setValue(30)                      
            self.current_task = "Сканирование реплеев завершено"
            self.status_label.setText(self.current_task)
        self.scan_completed.set()                                                 

    @Slot(str)
    def task_error(self, error_message):
        self.append_log(f"Ошибка выполнения задачи: {error_message}", False)
        QMessageBox.critical(self, "Ошибка задачи", f"Произошла ошибка:\n{error_message}")
        self.progress_bar.setValue(0)
        self.current_task = "Ошибка выполнения задачи"
        self.status_label.setText(self.current_task)
        self.scan_completed.set()                                          

    def browse_directory(self):
        folder = QFileDialog.getExistingDirectory(self, "Select osu! Game Directory", "")
        if folder:
            self.game_entry.setText(folder.replace("/", os.sep))
            self.append_log(f"Выбрана папка: {folder}", False)
                                           
            self.save_config()

    def start_all_processes(self):
                                                                                           
        game_dir = self.game_entry.text().strip()
        user_input = self.profile_entry.text().strip()

                                 
        if not game_dir or not user_input:
            QMessageBox.warning(self, "Ошибка", "Укажите папку osu! и ввод профиля (URL/ID/Ник).")
            return

                                              
        self.btn_all.setDisabled(True)
        self.browse_button.setDisabled(True)
        self.game_entry.setReadOnly(True)
        self.profile_entry.setReadOnly(True)
        self.scores_count_entry.setReadOnly(True)

                                       
        self.scan_completed.clear()
        self.top_completed.clear()
        self.img_completed.clear()
        self.overall_progress = 0
        self.progress_bar.setValue(0)

                                        
        self.current_task = "Запуск сканирования..."
        self.status_label.setText(self.current_task)

                                                                   
        threading.Thread(target=self._run_sequence, daemon=True).start()

    def _run_sequence(self):
                                                                
        try:
                                                      
            QtCore.QMetaObject.invokeMethod(
                self.btn_scan, "click",
                QtCore.Qt.ConnectionType.QueuedConnection
            )
            self.scan_completed.wait()

                                                       
            QtCore.QMetaObject.invokeMethod(
                self.btn_top, "click",
                QtCore.Qt.ConnectionType.QueuedConnection
            )
            self.top_completed.wait()

                                                      
            QtCore.QMetaObject.invokeMethod(
                self.btn_img, "click",
                QtCore.Qt.ConnectionType.QueuedConnection
            )
            self.img_completed.wait()
                                                                             
            QtCore.QMetaObject.invokeMethod(
                self, "all_completed_successfully",
                QtCore.Qt.ConnectionType.QueuedConnection
            )
        except Exception as e:
            logger.error(f"Ошибка последовательного запуска: {e}")
            QtCore.QMetaObject.invokeMethod(
                self, "enable_all_button",
                QtCore.Qt.ConnectionType.QueuedConnection
            )

    @Slot()
    def all_completed_successfully(self):
                                                                  
        self.append_log("Все операции успешно завершены!", False)
        QMessageBox.information(self, "Готово", "Анализ завершен! Вы можете найти результаты в папке 'results'.")
        self.enable_all_button()

    @Slot()
    def enable_all_button(self):
                                           
        self.btn_all.setDisabled(False)
        self.browse_button.setDisabled(False)
        self.game_entry.setReadOnly(False)
        self.profile_entry.setReadOnly(False)
        self.scores_count_entry.setReadOnly(False)

    def start_scan(self):
        game_dir = self.game_entry.text().strip()
        user_input = self.profile_entry.text().strip()
        if not game_dir or not user_input:
            QMessageBox.warning(self, "Ошибка", "Укажите папку osu! и ввод профиля (URL/ID/Ник).")
            self.scan_completed.set()                          
            return

        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None:
            self.scan_completed.set()                          
            return

        self.append_log("Запуск сканирования реплеев...", False)
        self.progress_bar.setValue(0)

        worker = Worker(scan_replays, game_dir, identifier, lookup_key)
        worker.signals.progress.connect(self.update_progress_bar)
        worker.signals.log.connect(self.append_log)
        worker.signals.finished.connect(self.task_finished)
        worker.signals.error.connect(self.task_error)
        self.threadpool.start(worker)

    def start_top(self):
        game_dir = self.game_entry.text().strip()
        user_input = self.profile_entry.text().strip()
        if not game_dir or not user_input:
            QMessageBox.warning(self, "Ошибка", "Укажите папку osu! и ввод профиля (URL/ID/Ник).")
            self.top_completed.set()                          
            return

        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None:
            self.top_completed.set()                          
            return

        self.append_log("Генерация потенциального топа...", False)

        worker = Worker(make_top, game_dir, identifier, lookup_key)
        worker.signals.log.connect(self.append_log)
        worker.signals.progress.connect(self.update_progress_bar)
        worker.signals.finished.connect(self.top_finished)
        worker.signals.error.connect(self.top_error)
        self.threadpool.start(worker)

    @Slot()
    def top_finished(self):
        self.progress_bar.setValue(60)                                           
        self.current_task = "Потенциальный топ создан"
        self.status_label.setText(self.current_task)
        self.top_completed.set()                                         

    @Slot(str)
    def top_error(self, error_message):
        self.append_log(f"Ошибка создания топа: {error_message}", False)
        QMessageBox.critical(self, "Ошибка", f"Произошла ошибка:\n{error_message}")
        self.progress_bar.setValue(30)                   
        self.current_task = "Ошибка создания топа"
        self.status_label.setText(self.current_task)
        self.top_completed.set()                                          

    def start_img(self):
        user_input = self.profile_entry.text().strip()
        scores_count = self.scores_count_entry.text().strip()

        if not user_input:
            QMessageBox.warning(self, "Ошибка", "Укажите ввод профиля (URL/ID/Ник).")
            self.img_completed.set()                          
            return

                                      
        self.ensure_csv_files_exist()

                                                                                   
        try:
            scores_count = int(scores_count) if scores_count else 10
                                   
            scores_count = max(1, min(100, scores_count))
        except ValueError:
            scores_count = 10
            self.scores_count_entry.setText("10")                              

        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None:
            self.img_completed.set()                          
            return

        self.append_log("Генерация изображений...", False)

        def task(user_id_or_name, key_type, num_scores):
            try:
                                            
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_progress_bar",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(int, 65),
                    QtCore.Q_ARG(int, 100)
                )

                                              
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_task",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, "Получение токена API...")
                )

                token = img_mod.get_token_osu()
                if not token:
                    raise ValueError("Не удалось получить токен API osu!")

                               
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_progress_bar",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(int, 70),
                    QtCore.Q_ARG(int, 100)
                )

                                              
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_task",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, "Получение данных пользователя...")
                )

                user_data = img_mod.get_user_osu(user_id_or_name, key_type, token)
                if not user_data:
                    error_msg = f"Не удалось получить данные пользователя '{user_id_or_name}' (тип: {key_type})."
                    QtCore.QMetaObject.invokeMethod(
                        self,
                        "img_error",
                        QtCore.Qt.ConnectionType.QueuedConnection,
                        QtCore.Q_ARG(str, error_msg)
                    )
                    return

                                              
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_progress_bar",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(int, 75),
                    QtCore.Q_ARG(int, 100)
                )

                uid = user_data["id"]
                uname = user_data["username"]

                profile_link = f"https://osu.ppy.sh/users/{uid}"
                log_message = f"Найден пользователь: {uname} ({profile_link})"
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "append_log",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, log_message),
                    QtCore.Q_ARG(bool, False)
                )

                                              
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_task",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, "Создание изображения lost scores...")
                )

                img_mod.make_img_lost(user_id=uid, user_name=uname, max_scores=num_scores)
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_progress_bar",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(int, 85),
                    QtCore.Q_ARG(int, 100)
                )

                                              
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_task",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, "Создание изображения потенциального топа...")
                )

                img_mod.make_img_top(user_id=uid, user_name=uname, max_scores=num_scores)
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_progress_bar",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(int, 100),                           
                    QtCore.Q_ARG(int, 100)
                )

                QtCore.QMetaObject.invokeMethod(
                    self,
                    "img_finished",
                    QtCore.Qt.ConnectionType.QueuedConnection
                )

            except Exception as e:
                error_message = f"Ошибка в потоке генерации изображений: {e}"
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "img_error",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, error_message)
                )

        threading.Thread(target=task, args=(identifier, lookup_key, scores_count), daemon=True).start()

    @Slot(str)
    def update_task(self, task_message):
                                      
        self.current_task = task_message
        self.status_label.setText(task_message)

    @Slot()
    def img_finished(self):
        self.append_log("Изображения созданы (в папке 'results').", False)
        self.progress_bar.setValue(100)                    
        self.current_task = "Изображения созданы"
        self.status_label.setText(self.current_task)
        self.img_completed.set()                          

    @Slot(str)
    def img_error(self, error_message):
        self.append_log(f"Ошибка генерации изображений: {error_message}", False)
        QMessageBox.critical(self, "Ошибка генерации изображений",
                             f"Не удалось создать изображения.\n{error_message}")
        self.progress_bar.setValue(60)                   
        self.current_task = "Ошибка генерации изображений"
        self.status_label.setText(self.current_task)
        self.img_completed.set()                                          

                                          
    def _parse_user_input(self, user_input):
        identifier = user_input
        lookup_key = 'username'

        if user_input.startswith(('http://', 'https://')):
            try:
                parts = user_input.strip('/').split('/')
                if len(parts) >= 2 and parts[-2] == 'users':
                    identifier = parts[-1]
                elif len(parts) >= 1 and parts[-1].isdigit():
                    identifier = parts[-1]
                else:
                    raise IndexError("Не удалось извлечь ID/ник из URL")

            except IndexError:
                QMessageBox.warning(self, "Ошибка", f"Некорректный URL профиля: {user_input}")
                return None, None

            if identifier.isdigit():
                lookup_key = 'id'
            else:
                lookup_key = 'username'

        elif user_input.isdigit():
            identifier = user_input
            lookup_key = 'id'
        else:
            identifier = user_input
            lookup_key = 'username'

        return identifier, lookup_key

    def show_context_menu(self, widget, position):
        menu = QMenu()
        if isinstance(widget, QLineEdit):
            cut_action = menu.addAction("Вырезать")
            cut_action.triggered.connect(widget.cut)
            cut_action.setEnabled(widget.hasSelectedText())
            copy_action = menu.addAction("Копировать")
            copy_action.triggered.connect(widget.copy)
            copy_action.setEnabled(widget.hasSelectedText())
            paste_action = menu.addAction("Вставить")
            paste_action.triggered.connect(widget.paste)
            paste_action.setEnabled(PYPERCLIP_AVAILABLE and bool(pyperclip.paste()))
            menu.addSeparator()
            select_all_action = menu.addAction("Выделить все")
            select_all_action.triggered.connect(widget.selectAll)
        elif isinstance(widget, QTextEdit):
            cut_action = menu.addAction("Вырезать")
            cut_action.triggered.connect(widget.cut)
            cut_action.setEnabled(not widget.isReadOnly() and widget.textCursor().hasSelection())
            copy_action = menu.addAction("Копировать")
            copy_action.triggered.connect(widget.copy)
            copy_action.setEnabled(widget.textCursor().hasSelection())
            paste_action = menu.addAction("Вставить")
            paste_action.triggered.connect(widget.paste)
            paste_action.setEnabled(not widget.isReadOnly() and PYPERCLIP_AVAILABLE and bool(pyperclip.paste()))
            menu.addSeparator()
            select_all_action = menu.addAction("Выделить все")
            select_all_action.triggered.connect(widget.selectAll)

        if menu.actions():
            menu.exec(widget.mapToGlobal(position))

    def disable_buttons(self, disabled=True):
                                               
        self.btn_all.setDisabled(disabled)
        self.browse_button.setDisabled(disabled)
        self.game_entry.setReadOnly(disabled)
        self.profile_entry.setReadOnly(disabled)
        self.scores_count_entry.setReadOnly(disabled)

    def _try_auto_detect_osu_path(self):
                                                                                   
                                                           
        if 'osu_path' in self.config and self.config['osu_path']:
            saved_path = self.config['osu_path']
            if os.path.isdir(saved_path):
                self.game_entry.setText(saved_path.replace("/", os.sep))
                self.append_log(f"Загружен путь из конфигурации: {saved_path}", False)
                return

                                                                                 
        potential_paths = []

                                             
        if platform.system() == "Windows":
            local_app_data = os.getenv('LOCALAPPDATA')
            if local_app_data:
                potential_paths.append(os.path.join(local_app_data, 'osu!'))

                                           
            for drive in ['C:', 'D:', 'E:', 'F:']:
                try:
                    if os.path.exists(f"{drive}\\Users"):
                        for username in os.listdir(f"{drive}\\Users"):
                            user_appdata = f"{drive}\\Users\\{username}\\AppData\\Local\\osu!"
                            if os.path.isdir(user_appdata):
                                potential_paths.append(user_appdata)
                except Exception:
                                                                   
                    pass

                                       
        for path in potential_paths:
            if os.path.isdir(path):
                self.game_entry.setText(path.replace("/", os.sep))
                self.append_log(f"Папка osu! найдена автоматически: {path}", False)
                                          
                self.config['osu_path'] = path
                self.save_config()
                return

        self.append_log("Папка osu! не найдена автоматически. Укажите путь вручную.", False)


def create_gui():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    create_gui()