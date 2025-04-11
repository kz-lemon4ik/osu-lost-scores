                          

import sys
import os
import platform
import threading
import logging                               
from functools import partial
from datetime import datetime

                 
from PySide6 import QtWidgets, QtCore, QtGui
from PySide6.QtCore import Qt, Signal, QRunnable, QThreadPool, QObject, Slot
from PySide6.QtGui import QPixmap, QPainter, QFontDatabase, QAction, QIcon, QFont, QColor
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QProgressBar, QTextEdit, QFileDialog, QMessageBox, QMenu, QFrame
)

                           
try:
    import pyperclip
    PYPERCLIP_AVAILABLE = True
except ImportError:
    print("ПРЕДУПРЕЖДЕНИЕ: pyperclip не найден (pip install pyperclip). Копирование/вставка могут работать некорректно.")
    PYPERCLIP_AVAILABLE = False

                                   
                                                          
import generate_image as img_mod
from analyzer import scan_replays, make_top

                         
BASE_SRC_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ICON_PATH = os.path.join(BASE_SRC_PATH, "assets", "icons")
FONT_PATH = os.path.join(BASE_SRC_PATH, "assets", "fonts")
BACKGROUND_FOLDER_PATH = os.path.join(BASE_SRC_PATH, "assets", "background")
BACKGROUND_IMAGE_PATH = os.path.join(BACKGROUND_FOLDER_PATH, "bg.png")

               
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

                                  
class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("osu! Lost Scores Analyzer")
        self.setGeometry(100, 100, 650, 500)
        self.setFixedSize(650, 500)

        self.load_fonts()
        self.load_icons()
        self.load_background()
        self.initUI()
        self.threadpool = QThreadPool()
        print(f"Макс. потоков в пуле: {self.threadpool.maxThreadCount()}")
        self._try_auto_detect_osu_path()

    def load_fonts(self):
                                                                    
        font_db = QFontDatabase()
        fonts_loaded = 0
        if os.path.isdir(FONT_PATH):
            for filename in os.listdir(FONT_PATH):
                if filename.lower().endswith((".ttf", ".otf")):
                    font_id = font_db.addApplicationFont(os.path.join(FONT_PATH, filename))
                    if font_id != -1: fonts_loaded += 1
                    else: print(f" -> Ошибка загрузки шрифта: {filename}")
            if fonts_loaded > 0: print(f"Загружено {fonts_loaded} локальных шрифтов.")
            else: print(f"Локальные шрифты в {FONT_PATH} не загружены.")
        else: print(f"Папка со шрифтами не найдена: {FONT_PATH}")

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
            scaled_pixmap = self.background_pixmap.scaled(self.size(), Qt.AspectRatioMode.IgnoreAspectRatio, Qt.TransformationMode.SmoothTransformation)
            painter.drawPixmap(self.rect(), scaled_pixmap)
        else: painter.fillRect(self.rect(), QColor(BG_COLOR))
        painter.end()

    def initUI(self):
                                          
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(10)

        self.title_label = QLabel("osu! Lost Scores Analyzer", self)
        self.title_label.setObjectName("TitleLabel")
        self.title_label.setFont(self.title_font)
        self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(self.title_label)
        main_layout.addSpacing(15)

        input_layout = QtWidgets.QGridLayout()
        input_layout.setSpacing(5)
        input_layout.setVerticalSpacing(10)
        input_layout.setColumnStretch(0, 1)

        dir_label = QLabel("osu! Game Directory", self)
        dir_label.setFont(self.label_font)
        self.game_entry = QLineEdit(self)
        self.game_entry.setFont(self.entry_font)
        self.game_entry.setPlaceholderText("Path to your osu! installation folder...")

        self.browse_button = HoverButton("",
                                    self.icons.get("folder", {}).get("normal"),
                                    self.icons.get("folder", {}).get("hover"), self)
        self.browse_button.setObjectName("BrowseButton")
        self.browse_button.setToolTip("Browse for osu! directory")
        self.browse_button.clicked.connect(self.browse_directory)
        entry_size_policy = self.game_entry.sizePolicy()
        self.browse_button.setSizePolicy(QtWidgets.QSizePolicy.Policy.Fixed, entry_size_policy.verticalPolicy())

        input_layout.addWidget(dir_label, 0, 0, 1, 2)
        input_layout.addWidget(self.game_entry, 1, 0)
        input_layout.addWidget(self.browse_button, 1, 1)

        url_label = QLabel("Player Profile URL", self)
        url_label.setFont(self.label_font)
        self.profile_entry = QLineEdit(self)
        self.profile_entry.setFont(self.entry_font)
        self.profile_entry.setPlaceholderText("e.g., https://osu.ppy.sh/users/2")

        input_layout.addWidget(url_label, 2, 0, 1, 2)
        input_layout.addWidget(self.profile_entry, 3, 0, 1, 2)

        main_layout.addLayout(input_layout)
        main_layout.addSpacing(10)

        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)

        self.btn_scan = HoverButton(" Scan Replays", self.icons.get("scan", {}).get("normal"), self.icons.get("scan", {}).get("hover"), self)
        self.btn_scan.setFont(self.button_font); self.btn_scan.setFixedHeight(40); self.btn_scan.clicked.connect(self.start_scan)
        self.btn_top = HoverButton(" Potential Top", self.icons.get("trophy", {}).get("normal"), self.icons.get("trophy", {}).get("hover"), self)
        self.btn_top.setFont(self.button_font); self.btn_top.setFixedHeight(40); self.btn_top.clicked.connect(self.start_top)
        self.btn_img = HoverButton(" Image Report", self.icons.get("image", {}).get("normal"), self.icons.get("image", {}).get("hover"), self)
        self.btn_img.setFont(self.button_font); self.btn_img.setFixedHeight(40); self.btn_img.clicked.connect(self.start_img)

        button_layout.addWidget(self.btn_scan, 1); button_layout.addWidget(self.btn_top, 1); button_layout.addWidget(self.btn_img, 1)
        main_layout.addLayout(button_layout)
        main_layout.addSpacing(10)

        self.progress_bar = QProgressBar(self)
        self.progress_bar.setFixedHeight(16)
        self.progress_bar.setTextVisible(False); self.progress_bar.setRange(0, 100); self.progress_bar.setValue(0)
        main_layout.addWidget(self.progress_bar)
        self.status_label = QLabel("Готово", self)                   
        self.status_label.setObjectName("StatusLabel")                                
                                                                 
        status_font = QFont("Exo 2", 11)                              
        status_font.setItalic(True)
        self.status_label.setFont(status_font)
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)                          
                                                           
        self.status_label.setStyleSheet(f"QLabel#StatusLabel {{ color: {TEXT_COLOR}; background-color: transparent; }}")
        main_layout.addWidget(self.status_label)                      
        main_layout.addSpacing(10)

        log_label = QLabel("Log", self)
        log_label.setFont(self.label_font)
        main_layout.addWidget(log_label)

        log_container = QFrame()
        log_container.setObjectName("LogContainer")
        log_container.setFrameShape(QFrame.Shape.NoFrame)
        log_container.setAutoFillBackground(True)
        palette = log_container.palette()
        palette.setColor(QtGui.QPalette.ColorRole.Window, QtGui.QColor(FG_COLOR))
        log_container.setPalette(palette)

        self.log_textbox = QTextEdit()
        self.log_textbox.setFont(self.log_font)
        self.log_textbox.setReadOnly(True)
        self.log_textbox.setStyleSheet("QTextEdit { background-color: transparent; border: none; }")

        log_layout = QVBoxLayout(log_container)
        log_layout.addWidget(self.log_textbox)
        log_layout.setContentsMargins(0, 0, 0, 0)

        main_layout.addWidget(log_container, stretch=1)

        self.log_textbox.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu);
        self.log_textbox.customContextMenuRequested.connect(partial(self.show_context_menu, self.log_textbox))

        self.setStyleSheet(self.get_stylesheet())

        QApplication.processEvents()
        try:
             line_edit_height = self.game_entry.sizeHint().height()
             if line_edit_height > 5:
                  self.browse_button.setFixedHeight(line_edit_height)
                  self.browse_button.setFixedWidth(line_edit_height)
             else: self.browse_button.setFixedSize(30,30)
        except Exception as e_size: print(f"Не удалось установить размер кнопки Browse: {e_size}"); self.browse_button.setFixedSize(30,30)

        self.game_entry.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu); self.game_entry.customContextMenuRequested.connect(partial(self.show_context_menu, self.game_entry))
        self.profile_entry.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu); self.profile_entry.customContextMenuRequested.connect(partial(self.show_context_menu, self.profile_entry))

    def get_stylesheet(self):
                                                                 
                                                                              
                                                                                                        
        return f"""
            QWidget {{ background-color: transparent; color: {TEXT_COLOR}; }}
            QLabel {{ background-color: transparent; color: {TEXT_COLOR}; }}
            QLabel#TitleLabel {{ color: {ACCENT_COLOR}; }}

            QLineEdit {{
                background-color: {FG_COLOR}; color: {TEXT_COLOR};
                border: 1px solid {SUBTLE_BORDER_COLOR}; border-radius: 5px;
                padding: 5px;
            }}
            QLineEdit:hover {{ border: 1px solid {ACCENT_COLOR}; }}
            QLineEdit:focus {{ border-color: {ACCENT_COLOR}; }}
            QLineEdit::placeholder {{
                color: {PLACEHOLDER_COLOR};
                font-style: italic;
                font-size: 9px;
            }}

            QPushButton {{
                background-color: {FG_COLOR}; color: {TEXT_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR}; border-radius: 5px;
                padding: 5px; min-height: 30px; text-align: left; padding-left: 10px;
            }}
            /* УДАЛЕНО правило QPushButton[hovering="true"] */
            /* УДАЛЕНО правило QPushButton:hover */


            QPushButton#BrowseButton {{
                 padding: 1px; border-radius: 3px; border: 1px solid {NORMAL_BORDER_COLOR};
                 text-align: center; padding-left: 0px;
            }}
            QPushButton#BrowseButton:hover {{
                 border-color: {ACCENT_COLOR};
                 background-color: {FG_COLOR};
            }}

            QProgressBar {{
                background-color: {FG_COLOR}; color: {TEXT_COLOR};
                border: 1px solid {NORMAL_BORDER_COLOR};
                border-radius: 8px;
                text-align: center;
            }}
            QProgressBar::chunk {{
                background-color: {ACCENT_COLOR};
                border-radius: 7px;
            }}

            /* Стили для QFrame-контейнера лога */
            QFrame#LogContainer {{
                /* Фон управляется палитрой */
                border: 1px solid {SUBTLE_BORDER_COLOR}; /* Задаем рамку */
                border-radius: 5px; /* Задаем скругление */
            }}
            /* Hover эффект для рамки контейнера */
            QFrame#LogContainer:hover {{
                border: 1px solid {ACCENT_COLOR};
            }}

            /* Стили для скроллбара и меню - без изменений */
            QScrollBar:vertical {{ border: none; background: {FG_COLOR}; width: 8px; margin: 0; }}
            QScrollBar::handle:vertical {{ background: {NORMAL_BORDER_COLOR}; min-height: 20px; border-radius: 4px; }}
            QScrollBar::handle:vertical:hover {{ background: {ACCENT_COLOR}; }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ border: none; background: none; height: 0px; }}
            QScrollBar::up-arrow:vertical, QScrollBar::down-arrow:vertical {{ background: none; }}
            QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{ background: none; }}
            QMenu {{ background-color: {FG_COLOR}; color: {TEXT_COLOR}; border: 1px solid {NORMAL_BORDER_COLOR}; }}
            QMenu::item:selected {{ background-color: {ACCENT_COLOR}; color: {TEXT_COLOR}; }}
            QMenu::separator {{ height: 1px; background: {NORMAL_BORDER_COLOR}; margin-left: 5px; margin-right: 5px; }}
        """

                            

                                                                 
                                             
                                                                 
    @Slot(str, bool)
    def append_log(self, message, update_last):
                   
        try:
            if update_last:
                                                  
                                                             
                self.status_label.setText(message)
                                                   
            else:
                                                                 
                                                                                
                self.status_label.setText("")                                                      

                                                                    
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
        value = int((current / total) * 100) if total > 0 else 0; self.progress_bar.setValue(value)

    @Slot()
    def task_finished(self):
        print("Фоновая задача завершена."); self.progress_bar.setValue(0)

    @Slot(str)
    def task_error(self, error_message):
                                                             
        self.append_log(f"Ошибка выполнения задачи: {error_message}", False)
        QMessageBox.critical(self, "Ошибка задачи", f"Произошла ошибка:\n{error_message}")
        self.progress_bar.setValue(0)                                     

    def browse_directory(self):
        folder = QFileDialog.getExistingDirectory(self, "Select osu! Game Directory", "");
        if folder: self.game_entry.setText(folder.replace("/", os.sep)); self.append_log(f"Выбрана папка: {folder}", False)

                                                                                        
    def start_scan(self):
        game_dir = self.game_entry.text().strip()
        user_input = self.profile_entry.text().strip()
        if not game_dir or not user_input: QMessageBox.warning(self, "Ошибка", "Укажите папку osu! и ввод профиля (URL/ID/Ник)."); return

        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None: return                                                   

        self.append_log("Запуск сканирования реплеев...", False); self.progress_bar.setValue(0)
        worker = Worker(scan_replays, game_dir, identifier, lookup_key)
        worker.signals.progress.connect(self.update_progress_bar);
        worker.signals.log.connect(self.append_log);
        worker.signals.finished.connect(self.task_finished);
        worker.signals.finished.connect(lambda: QMessageBox.information(self, "Готово", "Анализ реплеев завершён!"));
        worker.signals.error.connect(self.task_error);
        self.threadpool.start(worker)

    def start_top(self):
        game_dir = self.game_entry.text().strip()
        user_input = self.profile_entry.text().strip()
        if not game_dir or not user_input: QMessageBox.warning(self, "Ошибка", "Укажите папку osu! и ввод профиля (URL/ID/Ник)."); return

        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None: return

        self.append_log("Генерация потенциального топа...", False); self.progress_bar.setValue(0)                  
        worker = Worker(make_top, game_dir, identifier, lookup_key)
        worker.signals.log.connect(self.append_log);
        worker.signals.finished.connect(self.task_finished);
        worker.signals.finished.connect(lambda: QMessageBox.information(self, "Готово", "Файл 'top_with_lost.csv' успешно создан/обновлен!"));
        worker.signals.error.connect(self.task_error);
        self.threadpool.start(worker)

    def start_img(self):
        user_input = self.profile_entry.text().strip()
        if not user_input: QMessageBox.warning(self, "Ошибка", "Укажите ввод профиля (URL/ID/Ник)."); return

        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None: return

        self.append_log("Генерация изображений...", False); self.progress_bar.setValue(0)                  

        def task(user_id_or_name, key_type):
            try:
                token = img_mod.get_token_osu()
                if not token:                  
                     raise ValueError("Не удалось получить токен API osu!")

                user_data = img_mod.get_user_osu(user_id_or_name, key_type, token)
                if not user_data:
                    error_msg = f"Не удалось получить данные пользователя '{user_id_or_name}' (тип: {key_type})."
                                                                     
                    QtCore.QMetaObject.invokeMethod(self, "task_error", QtCore.Qt.ConnectionType.QueuedConnection, QtCore.QGenericArgument("QString", error_msg))
                    return

                uid = user_data["id"]; uname = user_data["username"]

                                                                            
                profile_link = f"https://osu.ppy.sh/users/{uid}"
                log_message = f"Найден пользователь: {uname} ({profile_link})"
                QtCore.QMetaObject.invokeMethod(self, "append_log", QtCore.Qt.ConnectionType.QueuedConnection,
                                                QtCore.QGenericArgument("QString", log_message),
                                                QtCore.QGenericArgument("bool", False))

                                             
                img_mod.make_img_lost(user_id=uid, user_name=uname);
                img_mod.make_img_top(user_id=uid, user_name=uname);
                                    
                QtCore.QMetaObject.invokeMethod(self, "image_task_finished", QtCore.Qt.ConnectionType.QueuedConnection)

            except Exception as e:                              
                                                                 
                QtCore.QMetaObject.invokeMethod(self, "task_error", QtCore.Qt.ConnectionType.QueuedConnection, QtCore.QGenericArgument("QString", f"Ошибка в потоке генерации изображений: {e}"))


                         
        threading.Thread(target=task, args=(identifier, lookup_key), daemon=True).start()

                                                                   
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
                QMessageBox.warning(self, "Ошибка", f"Некорректный URL профиля: {user_input}"); return None, None

                                         
            if identifier.isdigit(): lookup_key = 'id'
            else: lookup_key = 'username'

        elif user_input.isdigit():
            identifier = user_input
            lookup_key = 'id'
        else:                      
            identifier = user_input
            lookup_key = 'username'

        return identifier, lookup_key
                                           


    @Slot()
    def image_task_finished(self):
        self.append_log("Изображения созданы (в папке 'results').", False);
        QMessageBox.information(self, "Готово", "Изображения 'lost_scores_result.png' и 'potential_top_result.png' успешно созданы/обновлены!")
        self.progress_bar.setValue(0)                  

    @Slot(str)                                                     
    def image_task_error(self, error_message):
                                                                
        self.append_log(f"Ошибка генерации изображений: {error_message}", False)
        QMessageBox.critical(self, "Ошибка генерации изображений", f"Не удалось создать изображения.\n{error_message}")
        self.progress_bar.setValue(0)                  

    def show_context_menu(self, widget, position):
                                                      
        menu = QMenu();
        if isinstance(widget, QLineEdit):
            cut_action = menu.addAction("Вырезать"); cut_action.triggered.connect(widget.cut); cut_action.setEnabled(widget.hasSelectedText())
            copy_action = menu.addAction("Копировать"); copy_action.triggered.connect(widget.copy); copy_action.setEnabled(widget.hasSelectedText())
            paste_action = menu.addAction("Вставить"); paste_action.triggered.connect(widget.paste); paste_action.setEnabled(PYPERCLIP_AVAILABLE and bool(pyperclip.paste()))
            menu.addSeparator()
            select_all_action = menu.addAction("Выделить все"); select_all_action.triggered.connect(widget.selectAll)
        elif isinstance(widget, QTextEdit):
            cut_action = menu.addAction("Вырезать"); cut_action.triggered.connect(widget.cut); cut_action.setEnabled(not widget.isReadOnly() and widget.textCursor().hasSelection())
            copy_action = menu.addAction("Копировать"); copy_action.triggered.connect(widget.copy); copy_action.setEnabled(widget.textCursor().hasSelection())
            paste_action = menu.addAction("Вставить"); paste_action.triggered.connect(widget.paste); paste_action.setEnabled(not widget.isReadOnly() and PYPERCLIP_AVAILABLE and bool(pyperclip.paste()))
            menu.addSeparator()
            select_all_action = menu.addAction("Выделить все"); select_all_action.triggered.connect(widget.selectAll)

        if menu.actions():
            menu.exec(widget.mapToGlobal(position))


    def _try_auto_detect_osu_path(self):
                                                              
        osu_path_found = None
        if platform.system() == "Windows":
            local_app_data = os.getenv('LOCALAPPDATA')
            if local_app_data:
                potential_path = os.path.join(local_app_data, 'osu!')
                if os.path.isdir(potential_path):
                    osu_path_found = potential_path
        if osu_path_found:
            self.game_entry.setText(osu_path_found.replace("/", os.sep))
            self.append_log(f"Папка osu! найдена автоматически: {osu_path_found}", False)
        else:
            self.append_log("Папка osu! не найдена автоматически. Укажите путь вручную.", False)


                         
def create_gui():
                                  
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

                     
if __name__ == "__main__":
     create_gui()