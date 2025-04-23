import os
import platform
import threading
import logging
import time
import json
import subprocess
import shutil
from functools import partial
from datetime import datetime
from utils import get_resource_path
from database import db_close, db_init
from file_parser import reset_in_memory_caches
from config import DB_FILE

from PySide6 import QtCore, QtGui
from PySide6.QtCore import Qt, Signal, QRunnable, QThreadPool, QObject, Slot, QPropertyAnimation, QEasingCurve
from PySide6.QtGui import QPixmap, QPainter, QFontDatabase, QIcon, QFont, QColor
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QProgressBar, QTextEdit, QFileDialog, QMessageBox, QMenu, QFrame,
    QDialog, QCheckBox
)

try:
    import pyperclip

    PYPERCLIP_AVAILABLE = True
except ImportError:
    print(
        "WARNING: pyperclip not found (pip install pyperclip). Copy/paste may not work correctly.")
    PYPERCLIP_AVAILABLE = False

import generate_image as img_mod
from analyzer import scan_replays, make_top

logger = logging.getLogger(__name__)

BASE_SRC_PATH = get_resource_path("")
ICON_PATH = get_resource_path(os.path.join("assets", "icons"))
FONT_PATH = get_resource_path(os.path.join("assets", "fonts"))
BACKGROUND_FOLDER_PATH = get_resource_path(os.path.join("assets", "background"))
BACKGROUND_IMAGE_PATH = get_resource_path(os.path.join("assets", "background", "bg.png"))
CONFIG_PATH = get_resource_path(os.path.join("config", "gui_config.json"))

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
            current_style = self.styleSheet()
            hover_style = f"QPushButton {{ background-color: {FG_COLOR}; border: 2px solid {ACCENT_COLOR}; border-radius: 5px; }}"
            if "hover" not in current_style.lower():
                self.setStyleSheet(current_style + hover_style)
        if self.hover_icon and not self.hover_icon.isNull():
            self.setIcon(self.hover_icon)
        super().enterEvent(event)

    def leaveEvent(self, event):
        if self.objectName() != "BrowseButton":
            pass
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


class ApiDialog(QDialog):
    def __init__(self, parent=None, client_id="", client_secret="", keys_currently_exist=False):
        super().__init__(parent)
        self.setWindowTitle("API Keys Configuration")
        self.setFixedSize(440, 340)                                            
        self.setStyleSheet(f"""
            QDialog {{ background-color: {BG_COLOR}; color: {TEXT_COLOR}; }}
            QLabel {{ color: {TEXT_COLOR}; }}
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

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        info_label = QLabel("Enter your osu! API credentials:")
        info_label.setFont(QFont("Exo 2", 12))
        layout.addWidget(info_label)

        id_layout = QVBoxLayout()
        id_layout.setSpacing(10)
        id_label = QLabel("Client ID:")
        id_label.setFont(QFont("Exo 2", 12))
        self.id_input = QLineEdit(client_id)
        self.id_input.setFont(QFont("Exo 2", 12))
        self.id_input.setMinimumHeight(35)
        id_layout.addWidget(id_label)
        id_layout.addWidget(self.id_input)
        layout.addLayout(id_layout)

        layout.addSpacing(10)

        secret_layout = QVBoxLayout()
        secret_layout.setSpacing(10)
        secret_label = QLabel("Client Secret:")
        secret_label.setFont(QFont("Exo 2", 12))

        self.secret_container = QFrame()
        self.secret_container.setMinimumHeight(40)
        self.secret_container.setStyleSheet(f"""
            QFrame {{
                background-color: {FG_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 5px;
            }}
            QFrame:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)

        self.secret_layout_container = QHBoxLayout(self.secret_container)
        self.secret_layout_container.setContentsMargins(10, 0, 10, 0)
        self.secret_layout_container.setSpacing(0)

        self.secret_input = QLineEdit(client_secret)
        self.secret_input.setFont(QFont("Exo 2", 12))
        self.secret_input.setMinimumHeight(35)
        self.secret_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.secret_input.setStyleSheet("""
            QLineEdit {
                background-color: transparent;
                border: none;
                padding: 5px;
            }
        """)

        self.show_secret_btn = FolderButton(
            QIcon(os.path.join(ICON_PATH, "eye_closed.png")),
            QIcon(os.path.join(ICON_PATH, "eye_closed_hover.png"))
        )
        self.show_secret_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.show_secret_btn.setFixedSize(30, 30)
        self.show_secret_btn.setStyleSheet("QPushButton { background: transparent; border: none; }")
        self.show_secret_btn.clicked.connect(self.toggle_secret_visibility)
        self.is_secret_visible = False

        self.secret_layout_container.addWidget(self.secret_input, 1)
        self.secret_layout_container.addWidget(self.show_secret_btn, 0)

        secret_layout.addWidget(secret_label)
        secret_layout.addWidget(self.secret_container)
        layout.addLayout(secret_layout)

        layout.addSpacing(15)

        self.help_label = QLabel(
            '<a href="https://osu.ppy.sh/home/account/edit#oauth" style="color:#ee4bbd;">How to get API keys?</a>')
        self.help_label.setFont(QFont("Exo 2", 11))
        self.help_label.setOpenExternalLinks(True)
        self.help_label.setVisible(not keys_currently_exist)                                          
        layout.addWidget(self.help_label)

                                                           
        self.clear_hint_label = QLabel("Tip: To delete saved API keys, leave both fields empty and click 'Save'.")
        self.clear_hint_label.setFont(QFont("Exo 2", 9))                          
        self.clear_hint_label.setStyleSheet(f"color: #A0A0A0;")              
        self.clear_hint_label.setWordWrap(True)
        self.clear_hint_label.setVisible(keys_currently_exist)                                       
        layout.addWidget(self.clear_hint_label)

        button_layout = QHBoxLayout()
        button_layout.setSpacing(15)

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setFont(QFont("Exo 2", 12))
        self.cancel_btn.setMinimumHeight(40)
        self.cancel_btn.clicked.connect(self.reject)

        self.save_btn = QPushButton("Save")
        self.save_btn.setFont(QFont("Exo 2", 12, QFont.Weight.Bold))
        self.save_btn.setMinimumHeight(40)
        self.save_btn.clicked.connect(self.accept)

        self.save_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {FG_COLOR};
                color: {TEXT_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 5px;
                padding: 5px;
                text-align: center;
            }}
            QPushButton:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)

        self.cancel_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {FG_COLOR};
                color: {TEXT_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 5px;
                padding: 5px;
                text-align: center;
            }}
            QPushButton:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)

        button_layout.addWidget(self.cancel_btn)
        button_layout.addWidget(self.save_btn)
        layout.addLayout(button_layout)

        self.id_input.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.id_input.customContextMenuRequested.connect(lambda pos: self.show_context_menu(self.id_input, pos))

        self.secret_input.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.secret_input.customContextMenuRequested.connect(lambda pos: self.show_context_menu(self.secret_input, pos))

    def toggle_secret_visibility(self):
        self.is_secret_visible = not self.is_secret_visible
        if self.is_secret_visible:
            self.secret_input.setEchoMode(QLineEdit.EchoMode.Normal)
            self.show_secret_btn.normal_icon = QIcon(os.path.join(ICON_PATH, "eye_open.png"))
            self.show_secret_btn.hover_icon = QIcon(os.path.join(ICON_PATH, "eye_open_hover.png"))
        else:
            self.secret_input.setEchoMode(QLineEdit.EchoMode.Password)
            self.show_secret_btn.normal_icon = QIcon(os.path.join(ICON_PATH, "eye_closed.png"))
            self.show_secret_btn.hover_icon = QIcon(os.path.join(ICON_PATH, "eye_closed_hover.png"))

        self.show_secret_btn.setIcon(self.show_secret_btn.normal_icon)

    def show_context_menu(self, widget, position):
        menu = QMenu()

        menu.setStyleSheet("""
            QMenu {
                background-color: #121212;
                color: white;
                border: 1px solid #333333;
                border-radius: 5px;
                padding: 5px;
            }
            QMenu::item {
                padding: 5px 15px;
                border-radius: 3px;
            }
            QMenu::item:selected {
                background-color: #333333; /* Цвет выделения при наведении/выборе */
            }
            QMenu::item:disabled {
                color: #666666; /* Цвет для неактивных пунктов */
            }
        """)

        if isinstance(widget, QLineEdit):
            cut_action = menu.addAction("Cut")
            cut_action.triggered.connect(widget.cut)

            cut_action.setEnabled(widget.hasSelectedText())

            copy_action = menu.addAction("Copy")
            copy_action.triggered.connect(widget.copy)

            copy_action.setEnabled(widget.hasSelectedText())

            paste_action = menu.addAction("Paste")
            paste_action.triggered.connect(widget.paste)

            if PYPERCLIP_AVAILABLE:
                paste_action.setEnabled(bool(pyperclip.paste()))
            else:

                paste_action.setEnabled(True)

            menu.addSeparator()

            select_all_action = menu.addAction("Select All")
            select_all_action.triggered.connect(widget.selectAll)

            select_all_action.setEnabled(bool(widget.text()))

        if menu.actions():
            menu.exec(widget.mapToGlobal(position))


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("osu! Lost Scores Analyzer")
        self.setGeometry(100, 100, 650, 500)
        self.setFixedSize(650, 500)

        self.scan_completed = threading.Event()
        self.top_completed = threading.Event()
        self.img_completed = threading.Event()
        self.has_error = False
        self.overall_progress = 0
        self.current_task = "Ready to start"

        self.load_fonts()
        self.load_icons()
        self.load_background()

        self.initUI()

        self.config = {}
        self.load_config()

        if 'osu_path' in self.config and self.config['osu_path']:
            self.game_entry.setText(self.config['osu_path'])
        if 'username' in self.config and self.config['username']:
            self.profile_entry.setText(self.config['username'])
        if 'scores_count' in self.config and self.config['scores_count']:
            self.scores_count_entry.setText(str(self.config['scores_count']))

        self.threadpool = QThreadPool()
        logger.info(f"Max threads in pool: {self.threadpool.maxThreadCount()}")

        self._try_auto_detect_osu_path()

        from osu_api import get_keys_from_keyring
        client_id, client_secret = get_keys_from_keyring()

        if not client_id or not client_secret:
            QtCore.QTimer.singleShot(500, self.show_first_run_api_dialog)

    def show_first_run_api_dialog(self):
        QMessageBox.information(self, "API Keys Required",
                                "Welcome to osu! Lost Scores Analyzer!\n\n"
                                "To use this application, you need to provide osu! API keys.\n"
                                "Please enter your API keys in the next dialog.")
        self.open_api_dialog()

    def ensure_csv_files_exist(self):

        csv_dir = get_resource_path("csv")
        os.makedirs(csv_dir, exist_ok=True)

        lost_scores_path = os.path.join(csv_dir, "lost_scores.csv")
        if not os.path.exists(lost_scores_path):
            try:
                with open(lost_scores_path, "w", encoding="utf-8") as f:
                    f.write("PP,Beatmap ID,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,Rank\n")
                self.append_log("Created empty file lost_scores.csv", False)
            except Exception as e:
                self.append_log(f"Error creating lost_scores.csv: {e}", False)

        parsed_top_path = os.path.join(csv_dir, "parsed_top.csv")
        if not os.path.exists(parsed_top_path):
            try:
                with open(parsed_top_path, "w", encoding="utf-8") as f:
                    f.write(
                        "PP,Beatmap ID,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,weight_%,weight_PP,Score ID,Rank\n")
                self.append_log("Created empty file parsed_top.csv", False)
            except Exception as e:
                self.append_log(f"Error creating parsed_top.csv: {e}", False)

        top_with_lost_path = os.path.join(csv_dir, "top_with_lost.csv")
        if not os.path.exists(top_with_lost_path):
            try:
                with open(top_with_lost_path, "w", encoding="utf-8") as f:
                    f.write(
                        "PP,Beatmap ID,Status,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,Rank,weight_%,weight_PP,Score ID\n")
                self.append_log("Created empty file top_with_lost.csv", False)
            except Exception as e:
                self.append_log(f"Error creating top_with_lost.csv: {e}", False)

    def load_config(self):

        self.config = {}
        try:
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
                logger.info(f"Configuration loaded from {CONFIG_PATH}")

        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            self.config = {}

        if 'include_unranked' in self.config:
            self.include_unranked_checkbox.setChecked(self.config['include_unranked'])
        if 'show_lost' in self.config:
            self.show_lost_checkbox.setChecked(self.config['show_lost'])
        if 'clean_scan' in self.config:
            self.clean_scan_checkbox.setChecked(self.config['clean_scan'])

    def save_config(self):

        try:

            self.config['osu_path'] = self.game_entry.text().strip()
            self.config['username'] = self.profile_entry.text().strip()

            scores_count = self.scores_count_entry.text().strip()
            if scores_count:
                try:
                    self.config['scores_count'] = int(scores_count)
                except ValueError:
                    self.config['scores_count'] = 10

            self.config['include_unranked'] = self.include_unranked_checkbox.isChecked()
            self.config['show_lost'] = self.show_lost_checkbox.isChecked()
            self.config['clean_scan'] = self.clean_scan_checkbox.isChecked()

            with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=4)

            print(f"Configuration saved to {CONFIG_PATH}")
        except Exception as e:
            print(f"Error saving configuration: {e}")

    def closeEvent(self, event):
                                
        self.save_config()

                                                                 
        try:
            from database import db_close
            db_close()
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

        event.accept()

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
                        print(f" -> Error loading font: {filename}")
            if fonts_loaded > 0:
                print(f"Loaded {fonts_loaded} local fonts.")
            else:
                print(f"Local fonts in {FONT_PATH} not loaded.")
        else:
            print(f"Font folder not found: {FONT_PATH}")

        self.title_font = QFont("Exo 2", 24, QFont.Weight.Bold)
        self.button_font = QFont("Exo 2", 14, QFont.Weight.Bold)
        self.label_font = QFont("Exo 2", 14)
        self.entry_font = QFont("Exo 2", 10, weight=QFont.Weight.Normal, italic=True)
        self.log_font = QFont("Exo 2", 10)
        self.log_font.setItalic(True)

    def load_icons(self):
        self.icons = {}
        icon_files_qt = {
            "folder": {"normal": "folder.png", "hover": "folder_hover.png"}
        }
        for name, states in icon_files_qt.items():
            self.icons[name] = {}
            for state, filename in states.items():
                path = os.path.join(ICON_PATH, filename)
                if os.path.exists(path):
                    self.icons[name][state] = QIcon(path)
                else:
                    logger.warning(f"Icon file not found: {path}")
                    self.icons[name][state] = QIcon()

    def load_background(self):

        self.background_pixmap = None
        if os.path.exists(BACKGROUND_IMAGE_PATH):
            try:
                self.background_pixmap = QPixmap(BACKGROUND_IMAGE_PATH)
                if self.background_pixmap.isNull():
                    self.background_pixmap = None
                    print(f"Failed to load background: {BACKGROUND_IMAGE_PATH}")
                else:
                    print("Background image loaded.")
            except Exception as e:
                print(f"Error loading background: {e}")
                self.background_pixmap = None
        else:
            print(f"Background file not found: {BACKGROUND_IMAGE_PATH}")

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

        window_height = 785
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

        url_label = QLabel("Username (or ID / URL)", self)
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
        self.scores_count_entry.setGeometry(50, 305, 350, 40)
        self.api_button = HoverButton("API Keys", None, None, self)
        self.api_button.setGeometry(410, 305, 190, 40)
        self.api_button.setFont(self.button_font)
        self.api_button.setStyleSheet(f"""
            QPushButton {{
                background-color: {FG_COLOR};
                color: {TEXT_COLOR};
                border: 2px solid {ACCENT_COLOR};
                border-radius: 5px;
                text-align: center;
            }}
            QPushButton:hover {{
                border: 2px solid {ACCENT_COLOR};
                background-color: {FG_COLOR};
            }}
        """)
        self.api_button.clicked.connect(self.open_api_dialog)
        checkbox_y = 365

        self.include_unranked_checkbox = QCheckBox("Include unranked/loved beatmaps", self)
        self.include_unranked_checkbox.setGeometry(50, checkbox_y, 550, 25)
        self.include_unranked_checkbox.setFont(self.label_font)
        self.include_unranked_checkbox.setStyleSheet(f"""
            QCheckBox {{
                color: {TEXT_COLOR};
            }}
            QCheckBox::indicator {{
                width: 16px;
                height: 16px;
                background-color: {FG_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 3px;
            }}
            QCheckBox::indicator:checked {{
                background-color: {ACCENT_COLOR};
                border: 2px solid {ACCENT_COLOR};
            }}
            QCheckBox::indicator:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)

        self.show_lost_checkbox = QCheckBox("Show at least one lost score", self)
        self.show_lost_checkbox.setGeometry(50, checkbox_y + 35, 550, 25)
        self.show_lost_checkbox.setFont(self.label_font)
        self.show_lost_checkbox.setStyleSheet(f"""
            QCheckBox {{
                color: {TEXT_COLOR};
            }}
            QCheckBox::indicator {{
                width: 16px;
                height: 16px;
                background-color: {FG_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 3px;
            }}
            QCheckBox::indicator:checked {{
                background-color: {ACCENT_COLOR};
                border: 2px solid {ACCENT_COLOR};
            }}
            QCheckBox::indicator:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)

        self.clean_scan_checkbox = QCheckBox("Perform clean scan (reset cache)", self)
        self.clean_scan_checkbox.setGeometry(50, checkbox_y + 70, 550, 25)
        self.clean_scan_checkbox.setFont(self.label_font)
        self.clean_scan_checkbox.setStyleSheet(f"""
            QCheckBox {{
                color: {TEXT_COLOR};
            }}
            QCheckBox::indicator {{
                width: 16px;
                height: 16px;
                background-color: {FG_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 3px;
            }}
            QCheckBox::indicator:checked {{
                background-color: {ACCENT_COLOR};
                border: 2px solid {ACCENT_COLOR};
            }}
            QCheckBox::indicator:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)

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

        self.action_scan = QPushButton(self)
        self.action_scan.setGeometry(0, 0, 0, 0)
        self.action_scan.clicked.connect(self.start_scan)

        self.action_top = QPushButton(self)
        self.action_top.setGeometry(0, 0, 0, 0)
        self.action_top.clicked.connect(self.start_top)

        self.action_img = QPushButton(self)
        self.action_img.setGeometry(0, 0, 0, 0)
        self.action_img.clicked.connect(self.start_img)

        btn_all_width = 550
        btn_y = 470
        self.btn_all = HoverButton("Start Scan", None, None, self)
        self.btn_all.setGeometry(50, btn_y, 550, 50)
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
            }}
        """)

        self.api_button.setStyleSheet(f"""
            QPushButton {{
                background-color: {FG_COLOR};
                color: {TEXT_COLOR};
                border: 2px solid {NORMAL_BORDER_COLOR};
                border-radius: 5px;
                text-align: center;
            }}
            QPushButton:hover {{
                border: 2px solid {ACCENT_COLOR};
            }}
        """)
        self.btn_all.clicked.connect(self.start_all_processes)

        self.progress_bar = AnimatedProgressBar(self)
        self.progress_bar.setGeometry(50, btn_y + 65, 550, 20)
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
        self.status_label.setGeometry(50, btn_y + 90, 550, 25)
        self.status_label.setObjectName("StatusLabel")
        status_font = QFont("Exo 2", 11)
        status_font.setItalic(True)
        self.status_label.setFont(status_font)
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_label.setStyleSheet(f"QLabel#StatusLabel {{ color: {TEXT_COLOR}; background-color: transparent; }}")

        log_label = QLabel("Log", self)
        log_label.setGeometry(50, btn_y + 130, 550, 25)
        log_label.setFont(self.label_font)

        log_container = QFrame(self)
        log_container.setGeometry(50, btn_y + 160, 550, 120)
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
            error_logger.exception(f"Exception inside append_log when processing message '{message}': {e}")

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
        print("Background task completed.")

        if not self.scan_completed.is_set():
            self.progress_bar.setValue(30)
            self.current_task = "Replay scanning completed"
            self.status_label.setText(self.current_task)
        self.scan_completed.set()

    @Slot(str)
    def task_error(self, error_message):
        self.append_log(f"Task execution error: {error_message}", False)
        QMessageBox.critical(self, "Task Error", f"An error occurred:\n{error_message}")
        self.progress_bar.setValue(0)
        self.current_task = "Task execution error"
        self.status_label.setText(self.current_task)

        self.has_error = True

        self.scan_completed.set()
        self.top_completed.set()
        self.img_completed.set()

        self.enable_all_button()

    def browse_directory(self):
        folder = QFileDialog.getExistingDirectory(self, "Select osu! Game Directory", "")
        if folder:
            self.game_entry.setText(folder.replace("/", os.sep))
            self.append_log(f"Selected folder: {folder}", False)

            self.save_config()

    def start_all_processes(self):

        game_dir = self.game_entry.text().strip()
        user_input = self.profile_entry.text().strip()

        if not game_dir or not user_input:
            QMessageBox.warning(self, "Error", "Please specify osu! folder and profile input (URL/ID/Username).")
            return

        if not os.path.isdir(game_dir):
            QMessageBox.warning(self, "Error", f"Specified directory doesn't exist: {game_dir}")
            return

        songs_dir = os.path.join(game_dir, "Songs")
        replays_dir = os.path.join(game_dir, "Data", "r")

        if not os.path.isdir(songs_dir):
            QMessageBox.warning(self, "Error", f"Songs directory not found: {songs_dir}")
            return

        if not os.path.isdir(replays_dir):
            QMessageBox.warning(self, "Error", f"Replays directory not found: {replays_dir}")
            return

        self.has_error = False

        if self.clean_scan_checkbox.isChecked():
            self.append_log("Performing clean scan (cache reset)...", False)
            try:
                self.append_log("Closing database connection before cleaning...", False)
                db_close()
                logger.info("Database connection closed for cache cleaning.")

                db_path = get_resource_path(DB_FILE.replace("../", ""))
                if os.path.exists(db_path):
                    try:
                        os.remove(db_path)
                        self.append_log(f"Database file removed: {db_path}", False)
                    except Exception as e:
                        self.append_log(f"Failed to delete database file: {e}", False)

                project_root = get_resource_path("..")
                folders_to_clean = [
                    get_resource_path("cache"),
                    get_resource_path("maps"),
                    get_resource_path("results"),
                    get_resource_path("csv"),
                    get_resource_path("assets/images")
                ]
                logger.debug(f"Folders to clean absolute paths: {folders_to_clean}")

                for folder in folders_to_clean:
                    abs_folder_path = os.path.abspath(folder)                                                
                    if os.path.exists(abs_folder_path):
                        self.append_log(f"Deleting folder: {abs_folder_path}", False)
                        try:
                            shutil.rmtree(abs_folder_path)
                        except OSError as e:
                            logger.error(f"Error removing directory {abs_folder_path}: {e}")
                                                                                                         
                            if os.path.isdir(abs_folder_path):
                                for item in os.listdir(abs_folder_path):
                                    item_path = os.path.join(abs_folder_path, item)
                                    try:
                                        if os.path.isfile(item_path) or os.path.islink(item_path):
                                            os.unlink(item_path)
                                        elif os.path.isdir(item_path):
                                            shutil.rmtree(item_path)
                                    except Exception as ex_inner:
                                        logger.error(f"Failed to delete item {item_path}: {ex_inner}")
                                                                                            
                                        raise e from ex_inner                                 
                            else:
                                raise                                                    

                                                                                                             
                        if not os.path.exists(abs_folder_path):
                            os.makedirs(abs_folder_path, exist_ok=True)
                            self.append_log(f"Folder recreated: {abs_folder_path}", False)
                    else:
                                                                  
                        os.makedirs(abs_folder_path, exist_ok=True)
                        self.append_log(f"Folder created (did not exist): {abs_folder_path}", False)

                self.append_log("Re-initializing database connection after cleaning...", False)
                db_init()                                                           
                logger.info("Database re-initialized after cache cleaning.")

                self.append_log("Resetting in-memory caches...", False)
                reset_in_memory_caches()

                self.ensure_csv_files_exist()                                     
                self.append_log("Cache clearing completed successfully", False)

            except (FileNotFoundError, PermissionError, OSError) as e:
                self.append_log(f"Error clearing cache: {e}", False)
                                                                                      
                                                                              
                try:
                    self.append_log("Attempting DB re-initialization after cache error...", False)
                    db_init()
                except Exception as db_err:
                    self.append_log(f"Failed to re-initialize DB after cache error: {db_err}", False)
                                                                                         
                self.enable_all_button()                                          
                return                                            

            except Exception as e:
                self.append_log(f"Unexpected error clearing cache: {e}", False)
                self.enable_all_button()
                return

        self.btn_all.setDisabled(True)
        self.browse_button.setDisabled(True)
        self.api_button.setDisabled(True)
        self.game_entry.setReadOnly(True)
        self.profile_entry.setReadOnly(True)
        self.scores_count_entry.setReadOnly(True)
        self.include_unranked_checkbox.setEnabled(False)
        self.show_lost_checkbox.setEnabled(False)
        self.clean_scan_checkbox.setEnabled(False)

        self.scan_completed.clear()
        self.top_completed.clear()
        self.img_completed.clear()
        self.overall_progress = 0
        self.progress_bar.setValue(0)

        self.current_task = "Starting scan..."
        self.status_label.setText(self.current_task)
        self.append_log("Starting analysis...", False)

        threading.Thread(target=self._run_sequence, daemon=True).start()

    def _run_sequence(self):

        try:

            QtCore.QMetaObject.invokeMethod(
                self.action_scan, "click",
                QtCore.Qt.ConnectionType.QueuedConnection
            )

            max_wait_time = 3600
            wait_start = time.time()

            while not self.scan_completed.is_set():

                if time.time() - wait_start > max_wait_time:
                    logger.error("Maximum wait time exceeded for replay scanning")
                    QtCore.QMetaObject.invokeMethod(
                        self, "task_error",
                        QtCore.Qt.ConnectionType.QueuedConnection,
                        QtCore.Q_ARG(str, "Scan timeout exceeded")
                    )
                    return

                time.sleep(0.1)

            if self.has_error:
                logger.error("Scanning completed with error, aborting sequence")
                return

            QtCore.QMetaObject.invokeMethod(
                self.action_top, "click",
                QtCore.Qt.ConnectionType.QueuedConnection
            )

            wait_start = time.time()

            while not self.top_completed.is_set():
                if time.time() - wait_start > max_wait_time:
                    logger.error("Maximum wait time exceeded for potential top creation")
                    QtCore.QMetaObject.invokeMethod(
                        self, "task_error",
                        QtCore.Qt.ConnectionType.QueuedConnection,
                        QtCore.Q_ARG(str, "Top creation timeout exceeded")
                    )
                    return
                time.sleep(0.1)

            if self.has_error:
                logger.error("Top creation completed with error, aborting sequence")
                return

            QtCore.QMetaObject.invokeMethod(
                self.action_img, "click",
                QtCore.Qt.ConnectionType.QueuedConnection
            )

            wait_start = time.time()

            while not self.img_completed.is_set():
                if time.time() - wait_start > max_wait_time:
                    logger.error("Maximum wait time exceeded for image creation")
                    QtCore.QMetaObject.invokeMethod(
                        self, "task_error",
                        QtCore.Qt.ConnectionType.QueuedConnection,
                        QtCore.Q_ARG(str, "Image creation timeout exceeded")
                    )
                    return
                time.sleep(0.1)

            if not self.has_error:
                QtCore.QMetaObject.invokeMethod(
                    self, "all_completed_successfully",
                    QtCore.Qt.ConnectionType.QueuedConnection
                )
        except Exception as e:
            logger.error(f"Sequential launch error: {e}")
            QtCore.QMetaObject.invokeMethod(
                self, "task_error",
                QtCore.Qt.ConnectionType.QueuedConnection,
                QtCore.Q_ARG(str, f"Sequential launch error: {e}")
            )
        finally:

            QtCore.QMetaObject.invokeMethod(
                self, "enable_all_button",
                QtCore.Qt.ConnectionType.QueuedConnection
            )

    def open_folder(self, path):

        if platform.system() == "Windows":
            os.startfile(path)
        elif platform.system() == "Darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])

    @Slot()
    def all_completed_successfully(self):
        self.append_log("All operations completed successfully!", False)
        results_path = get_resource_path("results")

        if os.path.exists(results_path) and os.path.isdir(results_path):
            self.append_log(f"Opening results folder: {results_path}", False)
            self.open_folder(results_path)
        else:
            self.append_log(f"Results folder not found: {results_path}", False)

        QMessageBox.information(self, "Done", "Analysis completed! You can find results in the 'results' folder.")
        self.save_config()
        self.enable_all_button()

    @Slot()
    def enable_all_button(self):

        self.btn_all.setDisabled(False)
        self.browse_button.setDisabled(False)
        self.api_button.setDisabled(False)
        self.game_entry.setReadOnly(False)
        self.profile_entry.setReadOnly(False)
        self.scores_count_entry.setReadOnly(False)
        self.include_unranked_checkbox.setEnabled(True)
        self.show_lost_checkbox.setEnabled(True)
        self.clean_scan_checkbox.setEnabled(True)

    def start_scan(self):
        game_dir = self.game_entry.text().strip()
        user_input = self.profile_entry.text().strip()
        if not game_dir or not user_input:
            QMessageBox.warning(self, "Error", "Please specify osu! folder and profile input (URL/ID/Username).")
            self.scan_completed.set()
            return

        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None:
            self.scan_completed.set()
            return

        self.append_log("Starting replay scanning...", False)
        self.progress_bar.setValue(0)

        include_unranked = self.include_unranked_checkbox.isChecked()
        worker = Worker(scan_replays, game_dir, identifier, lookup_key, include_unranked=include_unranked)
        worker.signals.progress.connect(self.update_progress_bar)
        worker.signals.log.connect(self.append_log)
        worker.signals.finished.connect(self.task_finished)
        worker.signals.error.connect(self.task_error)
        self.threadpool.start(worker)

    def start_top(self):
        game_dir = self.game_entry.text().strip()
        user_input = self.profile_entry.text().strip()
        if not game_dir or not user_input:
            QMessageBox.warning(self, "Error", "Please specify osu! folder and profile input (URL/ID/Username).")
            self.top_completed.set()
            return

        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None:
            self.top_completed.set()
            return

        self.append_log("Generating potential top...", False)

        worker = Worker(make_top, game_dir, identifier, lookup_key)
        worker.signals.log.connect(self.append_log)
        worker.signals.progress.connect(self.update_progress_bar)
        worker.signals.finished.connect(self.top_finished)
        worker.signals.error.connect(self.top_error)
        self.threadpool.start(worker)

    @Slot()
    def top_finished(self):
        self.progress_bar.setValue(60)
        self.current_task = "Potential top created"
        self.status_label.setText(self.current_task)
        self.top_completed.set()

    @Slot(str)
    def top_error(self, error_message):
        self.append_log(f"Error creating top: {error_message}", False)
        QMessageBox.critical(self, "Error", f"An error occurred:\n{error_message}")
        self.progress_bar.setValue(30)
        self.current_task = "Error creating top"
        self.status_label.setText(self.current_task)
        self.top_completed.set()

    def start_img(self):
        user_input = self.profile_entry.text().strip()
        scores_count = self.scores_count_entry.text().strip()
        show_lost = self.show_lost_checkbox.isChecked()

        if not user_input:
            QMessageBox.warning(self, "Error", "Please specify profile input (URL/ID/Username).")
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

        self.append_log("Generating images...", False)

        def task(user_id_or_name, key_type, num_scores, show_lost_flag):
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
                    QtCore.Q_ARG(str, "Getting API token...")
                )

                token = img_mod.get_token_osu()
                if not token:
                    raise ValueError("Failed to get osu! API token!")

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
                    QtCore.Q_ARG(str, "Getting user data...")
                )

                user_data = img_mod.get_user_osu(user_id_or_name, key_type, token)
                if not user_data:
                    error_msg = f"Failed to get user data '{user_id_or_name}' (type: {key_type})."
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
                log_message = f"User found: {uname} ({profile_link})"
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
                    QtCore.Q_ARG(str, "Creating lost scores image...")
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
                    QtCore.Q_ARG(str, "Creating potential top image...")
                )

                img_mod.make_img_top(user_id=uid, user_name=uname, max_scores=num_scores, show_lost=show_lost_flag)
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
                error_message = f"Error in image generation thread: {e}"
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "img_error",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, error_message)
                )

        threading.Thread(target=task, args=(identifier, lookup_key, scores_count, show_lost), daemon=True).start()

    @Slot(str)
    def update_task(self, task_message):

        self.current_task = task_message
        self.status_label.setText(task_message)

    @Slot()
    def img_finished(self):
        self.append_log("Images created (in 'results' folder).", False)
        self.progress_bar.setValue(100)
        self.current_task = "Images created"
        self.status_label.setText(self.current_task)
        self.img_completed.set()

    @Slot(str)
    def img_error(self, error_message):
        self.append_log(f"Error generating images: {error_message}", False)
        QMessageBox.critical(self, "Error generating images",
                             f"Failed to create images.\n{error_message}")
        self.progress_bar.setValue(60)
        self.current_task = "Error generating images"
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
                    raise IndexError("Failed to extract ID/username from URL")

            except IndexError:
                QMessageBox.warning(self, "Error", f"Invalid profile URL: {user_input}")
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
        menu.setStyleSheet("""
            QMenu {
                background-color: #121212;
                color: white;
                border: 1px solid #333333;
                border-radius: 5px;
                padding: 5px;
            }
            QMenu::item {
                padding: 5px 15px;
                border-radius: 3px;
            }
            QMenu::item:selected {
                background-color: #333333;
            }
            QMenu::item:disabled {
                color: #666666;
            }
        """)

        if isinstance(widget, QLineEdit):
            cut_action = menu.addAction("Cut")
            cut_action.triggered.connect(widget.cut)

            copy_action = menu.addAction("Copy")
            copy_action.triggered.connect(widget.copy)

            paste_action = menu.addAction("Paste")
            paste_action.triggered.connect(widget.paste)

            menu.addSeparator()

            select_all_action = menu.addAction("Select All")
            select_all_action.triggered.connect(widget.selectAll)

        if menu.actions():
            menu.exec(widget.mapToGlobal(position))

    def disable_buttons(self, disabled=True):
        self.btn_all.setDisabled(disabled)
        self.browse_button.setDisabled(disabled)
        self.api_button.setDisabled(disabled)
        self.game_entry.setReadOnly(disabled)
        self.profile_entry.setReadOnly(disabled)
        self.scores_count_entry.setReadOnly(disabled)
        self.include_unranked_checkbox.setEnabled(not disabled)
        self.clean_scan_checkbox.setEnabled(not disabled)

    def _try_auto_detect_osu_path(self):

        if 'osu_path' in self.config and self.config['osu_path']:
            saved_path = self.config['osu_path']
            if os.path.isdir(saved_path):
                self.game_entry.setText(saved_path.replace("/", os.sep))
                self.append_log(f"Loaded path from configuration: {saved_path}", False)
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
                self.append_log(f"osu! folder automatically found: {path}", False)

                self.config['osu_path'] = path
                self.save_config()
                return

        self.append_log("osu! folder not found automatically. Please specify path manually.", False)

    def open_api_dialog(self):
        from osu_api import get_keys_from_keyring, save_keys_to_keyring, delete_keys_from_keyring

                                                                   
        current_client_id, current_client_secret = get_keys_from_keyring()
        keys_existed_before_dialog = bool(current_client_id and current_client_secret)

        dialog = ApiDialog(
            self,
            current_client_id or "",
            current_client_secret or "",
            keys_currently_exist=keys_existed_before_dialog
        )
        result = dialog.exec()

        if result == QDialog.DialogCode.Accepted:
            client_id = dialog.id_input.text().strip()
            client_secret = dialog.secret_input.text().strip()

                                  
            if not client_id and not client_secret:
                                                                      
                if keys_existed_before_dialog:
                    reply = QMessageBox.question(
                        self,
                        "Remove API Keys",
                        "You left both fields empty. Do you want to delete the saved API keys?",
                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                        QMessageBox.StandardButton.No
                    )

                    if reply == QMessageBox.StandardButton.Yes:
                        if delete_keys_from_keyring():
                            QMessageBox.information(self, "Success", "API keys have been removed successfully.")
                        else:
                            QMessageBox.critical(self, "Error", "Failed to remove API keys.")
                else:
                                                                       
                    QMessageBox.warning(
                        self,
                        "Empty API Keys",
                        "API keys cannot be empty. Please enter valid Client ID and Client Secret."
                    )
                return

                                                                                 
            if not client_id or not client_secret:
                QMessageBox.warning(
                    self,
                    "Incomplete API Keys",
                    "Both Client ID and Client Secret are required."
                )
                return

                                                      
            if save_keys_to_keyring(client_id, client_secret):
                QMessageBox.information(self, "Success", "API keys saved successfully!")
            else:
                QMessageBox.critical(self, "Error", "Failed to save API keys to system keyring.")

def create_gui():
    app = QApplication.instance()
    window = MainWindow()
    window.show()
    return window


if __name__ == "__main__":
    create_gui()
