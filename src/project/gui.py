import os
import sys
import platform
import threading
import logging
import time
import json
import subprocess
import shutil
import pandas as pd
import os.path
from functools import partial
from datetime import datetime
from utils import get_resource_path, mask_path_for_log
from database import db_close, db_init
from file_parser import reset_in_memory_caches
from config import DB_FILE, GUI_THREAD_POOL_SIZE

from PySide6 import QtCore, QtGui
from PySide6.QtCore import (
    Qt,
    Signal,
    QRunnable,
    QThreadPool,
    QObject,
    Slot,
    QPropertyAnimation,
    QEasingCurve,
    QAbstractTableModel,
    QModelIndex,
    QSize,
    QPoint,
    QRect,
)
from PySide6.QtGui import (
    QPixmap,
    QPainter,
    QFontDatabase,
    QIcon,
    QColor,
    QShortcut,
    QKeySequence,
)
from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QProgressBar,
    QTextEdit,
    QFileDialog,
    QMessageBox,
    QMenu,
    QFrame,
    QDialog,
    QCheckBox,
    QHeaderView,
    QTabWidget,
    QTableView,
    QSizePolicy,
    QToolTip,
)

try:
    import pyperclip

    PYPERCLIP_AVAILABLE = True
except ImportError:
    print(
        "WARNING: pyperclip not found (pip install pyperclip). Copy/paste may not work correctly."
    )
    PYPERCLIP_AVAILABLE = False

import generate_image as img_mod
from analyzer import scan_replays, make_top

logger = logging.getLogger(__name__)

BASE_SRC_PATH = get_resource_path("")
ICON_PATH = get_resource_path(os.path.join("assets", "icons"))
FONT_PATH = get_resource_path(os.path.join("assets", "fonts"))
BACKGROUND_FOLDER_PATH = get_resource_path(os.path.join("assets", "background"))
BACKGROUND_IMAGE_PATH = get_resource_path(
    os.path.join("assets", "background", "bg.png")
)
CONFIG_PATH = get_resource_path(os.path.join("config", "gui_config.json"))

os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)


def load_qss():
    style_path = get_resource_path(os.path.join("assets", "styles", "style.qss"))
    logger.debug(
        "Attempting to load QSS from: %s",
        mask_path_for_log(os.path.normpath(style_path)),
    )

    try:
        with open(style_path, "r", encoding="utf-8") as f:
            qss_content = f.read()
            logger.debug("QSS file successfully read (%d bytes).", len(qss_content))

            return qss_content
    except Exception as e:
        logger.debug("ERROR loading QSS file: %s", e)

        return ""


def show_api_limit_warning():
    from config import API_REQUESTS_PER_MINUTE

                                                               
    if 60 < API_REQUESTS_PER_MINUTE <= 1200:
        QMessageBox.warning(
            None,
            "API Rate Limit Warning",
            f"High API request rate detected\n\nCurrent setting: {API_REQUESTS_PER_MINUTE} requests per minute\n\n"
            f"WARNING: peppy prohibits using more than 60 requests per minute.\n"
            f"Burst spikes up to 1200 requests per minute are possible but at your own risk.\n"
            f"It may result in API/website usage ban."
        )

                                        
    elif API_REQUESTS_PER_MINUTE > 1200:
        QMessageBox.critical(
            None,
            "Excessive API Rate",
            f"Extremely high API request rate detected\n\nCurrent setting: {API_REQUESTS_PER_MINUTE} requests per minute\n\n"
            f"WARNING: This exceeds the maximum burst limit of 1200 requests per minute.\n"
            f"Program operation is not guaranteed - you will likely encounter 429 errors\n"
            f"and temporary API bans.\n\n"
            f"Please consider reducing API_REQUESTS_PER_MINUTE to at most 1200."
        )

                                      
    elif 0 < API_REQUESTS_PER_MINUTE < 60:
        QMessageBox.information(
            None,
            "Conservative API Rate",
            f"Low API request rate detected\n\nCurrent setting: {API_REQUESTS_PER_MINUTE} requests per minute\n\n"
            f"This is below the permitted rate of 60 requests per minute.\n"
            f"Consider setting API_REQUESTS_PER_MINUTE=60 for optimal performance."
        )

                                            
    elif API_REQUESTS_PER_MINUTE == 0:
        QMessageBox.critical(
            None,
            "No API Rate Limit",
            f"API rate limiting is disabled\n\n"
            f"You have disabled API rate limiting (API_REQUESTS_PER_MINUTE=0).\n\n"
            f"This is extremely dangerous and will almost certainly result in\n"
            f"your IP being temporarily banned from the osu! API.\n\n"
            f"Please set API_REQUESTS_PER_MINUTE to at least 1 and at most 1200."
        )

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
        if "progress_callback" in self.fn.__code__.co_varnames:
            self.kwargs["progress_callback"] = partial(self.emit_progress)
        if "gui_log" in self.fn.__code__.co_varnames:
            self.kwargs["gui_log"] = partial(self.emit_log)

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


class IconButton(QPushButton):
    def __init__(self, text, normal_icon=None, hover_icon=None, parent=None):
        super().__init__(text, parent)
        self.normal_icon = normal_icon if normal_icon else QIcon()
        self.hover_icon = hover_icon if hover_icon else QIcon()
        self.setIcon(self.normal_icon)
        self.setMouseTracking(True)

    def enterEvent(self, event):
        if self.hover_icon and not self.hover_icon.isNull():
            self.setIcon(self.hover_icon)
        super().enterEvent(event)

    def leaveEvent(self, event):
        if self.normal_icon and not self.normal_icon.isNull():
            self.setIcon(self.normal_icon)
        super().leaveEvent(event)


class FolderButton(QPushButton):
    def __init__(self, normal_icon=None, hover_icon=None, parent=None):
        super().__init__(parent)
        self.normal_icon = normal_icon if normal_icon else QIcon()
        self.hover_icon = hover_icon if hover_icon else QIcon()
        self.is_hovered = False
        self.setIcon(self.normal_icon)
        self.setMouseTracking(True)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setObjectName("browseButton")

    def enterEvent(self, event):
        self.is_hovered = True
        if self.hover_icon and not self.hover_icon.isNull():
            self.setIcon(self.hover_icon)
        super().enterEvent(event)

    def leaveEvent(self, event):
        self.is_hovered = False
        if self.normal_icon and not self.normal_icon.isNull():
            self.setIcon(self.normal_icon)
        super().leaveEvent(event)

    def mousePressEvent(self, event):
        super().mousePressEvent(event)

        if self.is_hovered:
            self.setIcon(self.hover_icon)
        else:
            self.setIcon(self.normal_icon)

    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)

        if self.is_hovered:
            self.setIcon(self.hover_icon)
        else:
            self.setIcon(self.normal_icon)


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
    def __init__(
        self, parent=None, client_id="", client_secret="", keys_currently_exist=False
    ):
        super().__init__(parent)
        self.setWindowTitle("API Keys Configuration")
        self.setFixedSize(440, 340)
        self.setObjectName("apiDialog")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        info_label = QLabel("Enter your osu! API credentials:")
        layout.addWidget(info_label)

        id_layout = QVBoxLayout()
        id_layout.setSpacing(10)
        id_label = QLabel("Client ID:")
        self.id_input = QLineEdit(client_id)
        self.id_input.setObjectName("idInput")
        self.id_input.setMinimumHeight(35)
        id_layout.addWidget(id_label)
        id_layout.addWidget(self.id_input)
        layout.addLayout(id_layout)

        layout.addSpacing(10)

        secret_layout = QVBoxLayout()
        secret_layout.setSpacing(10)
        secret_label = QLabel("Client Secret:")

        self.secret_container = QFrame()
        self.secret_container.setObjectName("secretContainer")
        self.secret_container.setMinimumHeight(40)

        self.secret_layout_container = QHBoxLayout(self.secret_container)
        self.secret_layout_container.setContentsMargins(10, 0, 10, 0)
        self.secret_layout_container.setSpacing(0)

        self.secret_input = QLineEdit(client_secret)
        self.secret_input.setObjectName("secretInput")
        self.secret_input.setMinimumHeight(35)
        self.secret_input.setEchoMode(QLineEdit.EchoMode.Password)

        self.show_secret_btn = FolderButton(
            QIcon(os.path.join(ICON_PATH, "eye_closed.png")),
            QIcon(os.path.join(ICON_PATH, "eye_closed_hover.png")),
        )
        self.show_secret_btn.setObjectName("showSecretBtn")
        self.show_secret_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.show_secret_btn.setFixedSize(30, 30)
        self.show_secret_btn.clicked.connect(self.toggle_secret_visibility)
        self.is_secret_visible = False

        self.secret_layout_container.addWidget(self.secret_input, 1)
        self.secret_layout_container.addWidget(self.show_secret_btn, 0)

        secret_layout.addWidget(secret_label)
        secret_layout.addWidget(self.secret_container)
        layout.addLayout(secret_layout)

        layout.addSpacing(15)

        self.help_label = QLabel(
            '<a href="https://osu.ppy.sh/home/account/edit#oauth" style="color:#ee4bbd;">How to get API keys?</a>'
        )
        self.help_label.setObjectName("helpLabel")
        self.help_label.setOpenExternalLinks(True)
        self.help_label.setVisible(not keys_currently_exist)
        layout.addWidget(self.help_label)

        self.clear_hint_label = QLabel(
            "Tip: To delete saved API keys, leave both fields empty and click 'Save'."
        )
        self.clear_hint_label.setObjectName("clearHintLabel")
        self.clear_hint_label.setWordWrap(True)
        self.clear_hint_label.setVisible(keys_currently_exist)
        layout.addWidget(self.clear_hint_label)

        button_layout = QHBoxLayout()
        button_layout.setSpacing(15)

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setObjectName("cancelBtn")
        self.cancel_btn.setMinimumHeight(40)
        self.cancel_btn.clicked.connect(self.reject)

        self.save_btn = QPushButton("Save")
        self.save_btn.setObjectName("saveBtn")
        self.save_btn.setMinimumHeight(40)
        self.save_btn.clicked.connect(self.accept)

        button_layout.addWidget(self.cancel_btn)
        button_layout.addWidget(self.save_btn)
        layout.addLayout(button_layout)

        self.id_input.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.id_input.customContextMenuRequested.connect(
            lambda pos: self.show_context_menu(self.id_input, pos)
        )

        self.secret_input.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.secret_input.customContextMenuRequested.connect(
            lambda pos: self.show_context_menu(self.secret_input, pos)
        )

    def toggle_secret_visibility(self):
        self.is_secret_visible = not self.is_secret_visible

        if self.is_secret_visible:
            self.secret_input.setEchoMode(QLineEdit.EchoMode.Normal)
            self.show_secret_btn.normal_icon = QIcon(
                os.path.join(ICON_PATH, "eye_open.png")
            )
            self.show_secret_btn.hover_icon = QIcon(
                os.path.join(ICON_PATH, "eye_open_hover.png")
            )
        else:
            self.secret_input.setEchoMode(QLineEdit.EchoMode.Password)
            self.show_secret_btn.normal_icon = QIcon(
                os.path.join(ICON_PATH, "eye_closed.png")
            )
            self.show_secret_btn.hover_icon = QIcon(
                os.path.join(ICON_PATH, "eye_closed_hover.png")
            )

        if self.show_secret_btn.is_hovered:
            self.show_secret_btn.setIcon(self.show_secret_btn.hover_icon)
        else:
            self.show_secret_btn.setIcon(self.show_secret_btn.normal_icon)

    def show_context_menu(self, widget, position):
        menu = QMenu()

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


class PandasTableModel(QAbstractTableModel):
    def __init__(self, data):
        super().__init__()
        self._data = data

    def rowCount(self, parent=QModelIndex()):
        return len(self._data)

    def columnCount(self, parent=QModelIndex()):
        return len(self._data.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None

        if role == Qt.ItemDataRole.DisplayRole:
            value = self._data.iloc[index.row(), index.column()]

            col_name = self._data.columns[index.column()]

            if col_name == "Rank":
                if value == "XH":
                    return "SSH"
                elif value == "X":
                    return "SS"

            if col_name == "Score ID":
                if pd.notna(value) and value != "LOST":
                    try:
                        return str(int(float(value)))
                    except (ValueError, TypeError):
                        return str(value)
                return str(value)

            if col_name == "Score":
                if pd.notna(value) and value != "":
                    try:
                        return str(int(float(value)))
                    except (ValueError, TypeError):
                        return str(value)
                return str(value)

            if isinstance(value, (float, int)):
                if col_name in ["100", "50", "Misses"]:
                    return str(int(value)) if pd.notna(value) else ""

                elif col_name in ["Accuracy"]:
                    return f"{value:.2f}"

                return str(value)

            return str(value)

        elif role == Qt.ItemDataRole.BackgroundRole:
            if index.row() % 2 == 0:
                return QColor("#302444")
            else:
                return QColor(45, 32, 62)

        elif role == Qt.ItemDataRole.TextAlignmentRole:
            value = self._data.iloc[index.row(), index.column()]
            if isinstance(value, (int, float)):
                return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            return Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter

        elif role == Qt.ItemDataRole.ForegroundRole:
            col_name = self._data.columns[index.column()]
            score_id_col = "Score ID" if "Score ID" in self._data.columns else None

            if score_id_col:
                try:
                    score_id_value = str(
                        self._data.iloc[
                            index.row(), self._data.columns.get_loc(score_id_col)
                        ]
                    )
                    if score_id_value == "LOST":
                        if col_name == "PP" or col_name == score_id_col:
                            return QColor("#ee4bbd")
                except Exception:
                    pass
            return QColor("#FFFFFF")

        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if (
            orientation == Qt.Orientation.Horizontal
            and role == Qt.ItemDataRole.DisplayRole
        ):
            if section < len(self._data.columns):
                return str(self._data.columns[section])
            return str(section)

        if (
            orientation == Qt.Orientation.Vertical
            and role == Qt.ItemDataRole.DisplayRole
        ):
            return str(section + 1)

        if (
            orientation == Qt.Orientation.Vertical
            and role == Qt.ItemDataRole.SizeHintRole
        ):
            return QSize(25, 25)

        return None

    def sort(self, column, order):
        try:
            if column >= len(self._data.columns):
                return

            col_name = self._data.columns[column]
            ascending = order == Qt.SortOrder.AscendingOrder

            self.layoutAboutToBeChanged.emit()

            if col_name == "Mods":
                temp_df = self._data.copy()

                def mod_sort_key(mod_str):
                    if not mod_str or pd.isna(mod_str):
                        return (0, "")

                    mods = mod_str.split(", ")

                    has_nc = "NC" in mods
                    if has_nc:
                        mods = [m for m in mods if m != "NC"]
                        mods.append("DT+")

                    if len(mods) == 1 and mods[0] == "NM":
                        mod_count = 0
                    else:
                        mod_count = len(mods)

                    mod_text = ", ".join(sorted(mods))
                    return (mod_count, mod_text)

                temp_df["mod_sort_key"] = temp_df[col_name].apply(mod_sort_key)
                self._data = temp_df.sort_values(
                    "mod_sort_key", ascending=ascending
                ).drop("mod_sort_key", axis=1)

            elif col_name == "Rank":
                rank_order = {
                    "XH": 0,
                    "SSH": 0,
                    "X": 1,
                    "SS": 1,
                    "SH": 2,
                    "S": 3,
                    "A": 4,
                    "B": 5,
                    "C": 6,
                    "D": 7,
                    "?": 8,
                    "": 9,
                }

                temp_df = self._data.copy()
                temp_df["rank_sort_key"] = temp_df[col_name].apply(
                    lambda r: rank_order.get(str(r).upper(), 9) if pd.notna(r) else 9
                )
                self._data = temp_df.sort_values(
                    "rank_sort_key", ascending=ascending
                ).drop("rank_sort_key", axis=1)

            elif col_name == "Score ID":
                temp_df = self._data.copy()

                def score_id_sort_key(id_str):
                    if str(id_str) == "LOST":
                        return 0 if not ascending else float("inf")
                    try:
                        return int(float(id_str))
                    except (ValueError, TypeError):
                        return id_str

                temp_df["id_sort_key"] = temp_df[col_name].apply(score_id_sort_key)
                self._data = temp_df.sort_values(
                    "id_sort_key", ascending=ascending
                ).drop("id_sort_key", axis=1)

            elif col_name == "Date":
                try:
                    temp_df = self._data.copy()

                    date_formats = [
                        "%d-%m-%Y %H:%M:%S",
                        "%d-%m-%Y",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d",
                    ]

                    def parse_date_safe(date_str):
                        if pd.isna(date_str):
                            return pd.NaT

                        date_str = str(date_str).strip()
                        if date_str.endswith("..."):
                            date_str = date_str[:-3].strip()

                        for fmt in date_formats:
                            try:
                                return pd.to_datetime(date_str, format=fmt)
                            except (ValueError, TypeError):
                                continue

                        try:
                            return pd.to_datetime(date_str, errors="coerce")
                        except Exception as e:
                            logger.debug(f"Unexpected error parsing date: {e}")
                            return pd.NaT

                    temp_df["date_sort_key"] = temp_df[col_name].apply(parse_date_safe)
                    self._data = temp_df.sort_values(
                        "date_sort_key", ascending=ascending, na_position="last"
                    ).drop("date_sort_key", axis=1)
                except Exception as e:
                    logger.error(f"Error sorting dates: {e}")

                    self._data = self._data.sort_values(
                        col_name, ascending=ascending, na_position="last"
                    )

            elif col_name in ["100", "50", "Misses"]:
                self._data[col_name] = pd.to_numeric(
                    self._data[col_name], errors="coerce"
                )
                self._data = self._data.sort_values(
                    col_name, ascending=ascending, na_position="last"
                )

            else:
                try:
                    temp_series = pd.to_numeric(self._data[col_name], errors="coerce")

                    if not temp_series.isna().all():
                        self._data[col_name] = temp_series.fillna(self._data[col_name])
                except Exception as e:
                    logger.debug(f"Column {col_name} is not numeric: {e}")

                self._data = self._data.sort_values(
                    col_name, ascending=ascending, na_position="last"
                )

            self.layoutChanged.emit()
        except Exception as e:
            logger.error(f"Error sorting table: {e}")


class ResultsWindow(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowFlags(
            Qt.WindowType.Window
            | Qt.WindowType.WindowSystemMenuHint
            | Qt.WindowType.WindowMinMaxButtonsHint
            | Qt.WindowType.WindowCloseButtonHint
        )

        screen = QApplication.primaryScreen().availableGeometry()
        self.resize(int(screen.width() * 0.8), int(screen.height() * 0.8))
        self.setWindowTitle("Full Scan Results")
        self.setObjectName("resultsWindow")

        logger.info("Initializing ResultsWindow")

        self.stats_data = {"lost_scores": {}, "parsed_top": {}, "top_with_lost": {}}

        self.search_results = []
        self.current_result_index = -1

        logger.info(f"Created stats_data dictionary: {self.stats_data}")

        self.setWindowFlags(
            Qt.WindowType.Dialog
            | Qt.WindowType.WindowSystemMenuHint
            | Qt.WindowType.WindowMinMaxButtonsHint
            | Qt.WindowType.WindowCloseButtonHint
        )

        screen = QApplication.primaryScreen().availableGeometry()
        self.resize(int(screen.width() * 0.8), int(screen.height() * 0.8))

        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(20, 20, 20, 20)
        self.layout.setSpacing(15)

        self.scan_time_label = QLabel("Last scan: Unknown")

        self.search_container = QFrame(self)
        self.search_container.setObjectName("searchContainer")
        self.search_container.setMinimumWidth(350)
        self.search_container.setMaximumHeight(40)

        search_layout = QHBoxLayout(self.search_container)
        search_layout.setContentsMargins(0, 0, 0, 0)
        search_layout.setSpacing(5)

        self.search_count_label = QLabel("", self.search_container)
        self.search_count_label.setObjectName("searchCountLabel")
        self.search_count_label.setMinimumWidth(60)
        self.search_count_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        search_layout.addWidget(self.search_count_label)

        self.prev_result_button = QPushButton("▲", self.search_container)
        self.prev_result_button.setObjectName("prevResultButton")
        self.prev_result_button.setMinimumHeight(30)
        self.prev_result_button.setMinimumWidth(30)
        self.prev_result_button.setMaximumWidth(30)
        self.prev_result_button.setMaximumHeight(30)
        self.prev_result_button.clicked.connect(self.go_to_previous_result)

        self.prev_result_button.setVisible(False)
        search_layout.addWidget(self.prev_result_button)

        self.next_result_button = QPushButton("▼", self.search_container)
        self.next_result_button.setObjectName("nextResultButton")
        self.next_result_button.setMinimumHeight(30)
        self.next_result_button.setMinimumWidth(30)
        self.next_result_button.setMaximumWidth(30)
        self.next_result_button.setMaximumHeight(30)
        self.next_result_button.clicked.connect(self.go_to_next_result)
        self.next_result_button.setVisible(False)
        search_layout.addWidget(self.next_result_button)

        self.search_input = QLineEdit(self.search_container)
        self.search_input.setObjectName("searchInput")
        self.search_input.setPlaceholderText("Search in table...")
        self.search_input.setMinimumHeight(30)

        search_layout.addWidget(self.search_input)

        self.search_input.returnPressed.connect(self.perform_search)

        self.search_input.setFocusPolicy(Qt.FocusPolicy.StrongFocus)

        self.search_input.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.search_input.customContextMenuRequested.connect(
            lambda pos: self.show_context_menu(self.search_input, pos)
        )

        self.search_button = QPushButton("Find", self.search_container)
        self.search_button.setObjectName("searchButton")
        self.search_button.setMinimumHeight(30)
        self.search_button.setMinimumWidth(70)
        self.search_button.clicked.connect(self.perform_search)

        search_layout.addWidget(self.search_button)

        title_layout = QHBoxLayout()
        title_layout.setContentsMargins(0, 0, 0, 5)
        title_layout.addWidget(self.scan_time_label, 1)
        title_layout.addWidget(self.search_container, 0)
        self.layout.addLayout(title_layout)

        self.search_input.returnPressed.connect(self.perform_search)

        self.tab_widget = QTabWidget()
        self.layout.addWidget(self.tab_widget)

        self.lost_scores_tab = QWidget()
        self.lost_scores_layout = QVBoxLayout(self.lost_scores_tab)
        self.lost_scores_layout.setContentsMargins(0, 0, 0, 0)
        self.lost_scores_layout.setSpacing(5)

        self.lost_scores_view = QTableView()
        self.lost_scores_view.setSortingEnabled(True)
        self.lost_scores_view.horizontalHeader().setStretchLastSection(False)
        self.lost_scores_view.verticalHeader().setDefaultSectionSize(30)
        self.lost_scores_view.verticalHeader().setMinimumWidth(25)
        self.lost_scores_view.verticalHeader().setMaximumWidth(35)

        self.lost_scores_view.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )

        self.lost_scores_layout.addWidget(self.lost_scores_view, 1)

        self.parsed_top_tab = QWidget()
        self.parsed_top_layout = QVBoxLayout(self.parsed_top_tab)
        self.parsed_top_layout.setContentsMargins(0, 0, 0, 0)
        self.parsed_top_layout.setSpacing(5)

        self.parsed_top_view = QTableView()
        self.parsed_top_view.setSortingEnabled(True)
        self.parsed_top_view.horizontalHeader().setStretchLastSection(False)
        self.parsed_top_view.verticalHeader().setDefaultSectionSize(30)
        self.parsed_top_view.verticalHeader().setMinimumWidth(25)
        self.parsed_top_view.verticalHeader().setMaximumWidth(35)

        self.parsed_top_view.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )

        self.parsed_top_layout.addWidget(self.parsed_top_view, 1)

        self.top_with_lost_tab = QWidget()
        self.top_with_lost_layout = QVBoxLayout(self.top_with_lost_tab)
        self.top_with_lost_layout.setContentsMargins(0, 0, 0, 0)
        self.top_with_lost_layout.setSpacing(5)

        self.top_with_lost_view = QTableView()
        self.top_with_lost_view.setSortingEnabled(True)
        self.top_with_lost_view.horizontalHeader().setStretchLastSection(False)
        self.top_with_lost_view.verticalHeader().setDefaultSectionSize(30)
        self.top_with_lost_view.verticalHeader().setMinimumWidth(25)
        self.top_with_lost_view.verticalHeader().setMaximumWidth(35)

        self.top_with_lost_view.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )

        self.top_with_lost_layout.addWidget(self.top_with_lost_view, 1)

        self.tab_widget.addTab(self.lost_scores_tab, "Lost Scores")
        self.tab_widget.addTab(self.parsed_top_tab, "Online Top")
        self.tab_widget.addTab(self.top_with_lost_tab, "Potential Top")

        self.bottom_layout = QHBoxLayout()
        self.bottom_layout.setContentsMargins(0, 5, 0, 0)
        self.layout.addLayout(self.bottom_layout)

        self.stats_panel = QFrame()
        self.stats_panel.setObjectName("StatsPanel")
        self.stats_panel.setMinimumHeight(40)
        self.stats_panel.setMaximumHeight(50)
        self.stats_panel.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed
        )

        self.stats_panel_layout = QHBoxLayout(self.stats_panel)
        self.stats_panel_layout.setContentsMargins(10, 5, 10, 5)
        self.stats_panel_layout.setSpacing(20)

        logger.info(f"Created stats_panel_layout: {self.stats_panel_layout}")

        self.bottom_layout.addWidget(self.stats_panel, 1)

        self.close_button = QPushButton("Close")
        self.close_button.setObjectName("closeButton")
        self.close_button.setMinimumWidth(120)
        self.close_button.setMinimumHeight(40)
        self.close_button.clicked.connect(self.close)

        self.bottom_layout.addWidget(self.close_button, 0)

        self.close_button.setAutoDefault(False)
        self.close_button.setDefault(False)
        self.search_button.setAutoDefault(True)
        self.search_button.setDefault(True)

        self.lost_scores_view.setSelectionMode(
            QTableView.SelectionMode.ExtendedSelection
        )
        self.parsed_top_view.setSelectionMode(
            QTableView.SelectionMode.ExtendedSelection
        )
        self.top_with_lost_view.setSelectionMode(
            QTableView.SelectionMode.ExtendedSelection
        )

        self.lost_scores_view.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu
        )
        self.parsed_top_view.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu
        )
        self.top_with_lost_view.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu
        )

        self.lost_scores_view.customContextMenuRequested.connect(
            lambda pos: self.show_table_context_menu(self.lost_scores_view, pos)
        )
        self.parsed_top_view.customContextMenuRequested.connect(
            lambda pos: self.show_table_context_menu(self.parsed_top_view, pos)
        )
        self.top_with_lost_view.customContextMenuRequested.connect(
            lambda pos: self.show_table_context_menu(self.top_with_lost_view, pos)
        )

        shortcut_search = QShortcut(QKeySequence("Ctrl+F"), self)
        shortcut_search.activated.connect(self.focus_search)

        shortcut_copy_lost = QShortcut(QKeySequence("Ctrl+C"), self.lost_scores_view)
        shortcut_copy_lost.activated.connect(
            lambda: self.copy_selected_cells(self.lost_scores_view)
        )

        shortcut_copy_top = QShortcut(QKeySequence("Ctrl+C"), self.parsed_top_view)
        shortcut_copy_top.activated.connect(
            lambda: self.copy_selected_cells(self.parsed_top_view)
        )

        shortcut_copy_potential = QShortcut(
            QKeySequence("Ctrl+C"), self.top_with_lost_view
        )
        shortcut_copy_potential.activated.connect(
            lambda: self.copy_selected_cells(self.top_with_lost_view)
        )

        self.stats_data = {"lost_scores": {}, "parsed_top": {}, "top_with_lost": {}}

        self.load_data()

        self.tab_widget.currentChanged.connect(self.update_stats_panel)
        self.update_stats_panel(self.tab_widget.currentIndex())

        current_tab_index = self.tab_widget.currentIndex()
        if current_tab_index == 0:
            self.lost_scores_view.setFocus()
        elif current_tab_index == 1:
            self.parsed_top_view.setFocus()
        else:
            self.top_with_lost_view.setFocus()

    def load_data(self):
        try:
            self.update_scan_time()

            lost_scores_path = get_resource_path(os.path.join("csv", "lost_scores.csv"))
            if os.path.exists(lost_scores_path):
                lost_scores_df = pd.read_csv(lost_scores_path)
                model = PandasTableModel(lost_scores_df)
                self.lost_scores_view.setModel(model)

                self.setup_column_widths(self.lost_scores_view)

                self.calculate_lost_scores_stats(lost_scores_df)
            else:
                empty_df = pd.DataFrame({"Status": ["No data found. Run scan first."]})
                model = PandasTableModel(empty_df)
                self.lost_scores_view.setModel(model)

            parsed_top_path = get_resource_path(os.path.join("csv", "parsed_top.csv"))
            if os.path.exists(parsed_top_path):
                try:
                    full_df = pd.read_csv(parsed_top_path)

                    stats_keywords = [
                        "Sum weight_PP",
                        "Overall PP",
                        "Difference",
                        "Overall Accuracy",
                    ]
                    stats_rows = full_df[
                        full_df.iloc[:, 0]
                        .astype(str)
                        .str.contains("|".join(stats_keywords), case=False, na=False)
                    ]

                    if not stats_rows.empty:
                        first_stats_idx = stats_rows.index.min()

                        stats_df = full_df.iloc[first_stats_idx:].copy()
                        data_df = full_df.iloc[:first_stats_idx].copy()

                        for _, row in stats_df.iterrows():
                            if pd.notna(row.iloc[0]) and pd.notna(row.iloc[1]):
                                self.stats_data["parsed_top"][str(row.iloc[0])] = str(
                                    row.iloc[1]
                                )
                    else:
                        data_df = full_df.copy()

                    model = PandasTableModel(data_df)
                    self.parsed_top_view.setModel(model)

                    self.setup_column_widths(self.parsed_top_view)

                except Exception as e:
                    logger.error(f"Error processing parsed_top.csv: {str(e)}")
                    empty_df = pd.DataFrame(
                        {"Error": [f"Error processing data: {str(e)}"]}
                    )
                    model = PandasTableModel(empty_df)
                    self.parsed_top_view.setModel(model)
            else:
                empty_df = pd.DataFrame({"Status": ["No data found. Run scan first."]})
                model = PandasTableModel(empty_df)
                self.parsed_top_view.setModel(model)

            top_with_lost_path = get_resource_path(
                os.path.join("csv", "top_with_lost.csv")
            )
            if os.path.exists(top_with_lost_path):
                try:
                    full_df = pd.read_csv(top_with_lost_path)

                    stats_keywords = [
                        "Sum weight_PP",
                        "Overall Potential PP",
                        "Difference",
                        "Overall Accuracy",
                        "Δ Overall Accuracy",
                    ]
                    stats_rows = full_df[
                        full_df.iloc[:, 0]
                        .astype(str)
                        .str.contains("|".join(stats_keywords), case=False, na=False)
                    ]

                    if not stats_rows.empty:
                        first_stats_idx = stats_rows.index.min()

                        stats_df = full_df.iloc[first_stats_idx:].copy()
                        data_df = full_df.iloc[:first_stats_idx].copy()

                        for _, row in stats_df.iterrows():
                            if pd.notna(row.iloc[0]) and pd.notna(row.iloc[1]):
                                self.stats_data["top_with_lost"][str(row.iloc[0])] = (
                                    str(row.iloc[1])
                                )
                    else:
                        data_df = full_df.copy()

                    model = PandasTableModel(data_df)
                    self.top_with_lost_view.setModel(model)

                    self.setup_column_widths(self.top_with_lost_view)

                    status_column_index = -1
                    for i in range(model.columnCount()):
                        if model.headerData(i, Qt.Orientation.Horizontal) == "Status":
                            status_column_index = i
                            break

                    if status_column_index >= 0:
                        self.top_with_lost_view.hideColumn(status_column_index)

                except Exception as e:
                    logger.error(f"Error processing top_with_lost.csv: {str(e)}")
                    empty_df = pd.DataFrame(
                        {"Error": [f"Error processing data: {str(e)}"]}
                    )
                    model = PandasTableModel(empty_df)
                    self.top_with_lost_view.setModel(model)
            else:
                empty_df = pd.DataFrame({"Status": ["No data found. Run scan first."]})
                model = PandasTableModel(empty_df)
                self.top_with_lost_view.setModel(model)

            self.update_stats_panel(self.tab_widget.currentIndex())

        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            error_df = pd.DataFrame({"Error": [f"Error loading data: {str(e)}"]})
            model = PandasTableModel(error_df)
            self.lost_scores_view.setModel(model)
            self.parsed_top_view.setModel(model)
            self.top_with_lost_view.setModel(model)

    def update_scan_time(self):
        try:
            csv_files = [
                get_resource_path(os.path.join("csv", "lost_scores.csv")),
                get_resource_path(os.path.join("csv", "parsed_top.csv")),
                get_resource_path(os.path.join("csv", "top_with_lost.csv")),
            ]

            newest_time = None
            for file_path in csv_files:
                if os.path.exists(file_path):
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if newest_time is None or file_time > newest_time:
                        newest_time = file_time

            if newest_time:
                self.scan_time_label.setText(
                    f"Last scan: {newest_time.strftime('%Y-%m-%d %H:%M:%S')}"
                )
            else:
                self.scan_time_label.setText("Last scan: Unknown")
        except Exception as e:
            logger.error(f"Error updating scan time: {str(e)}")
            self.scan_time_label.setText("Last scan: Error checking time")

    def setup_column_widths(self, table_view):
        try:
            header = table_view.horizontalHeader()
            model = table_view.model()
            if not model:
                return

            for i in range(model.columnCount()):
                header.setSectionResizeMode(i, QHeaderView.ResizeMode.Interactive)

            default_widths = {
                "PP": 60,
                "Beatmap ID": 80,
                "Status": 70,
                "Mods": 80,
                "100": 40,
                "50": 40,
                "Misses": 50,
                "Accuracy": 60,
                "Score": 80,
                "Date": 120,
                "weight_%": 70,
                "weight_PP": 70,
                "Score ID": 90,
                "Rank": 50,
            }

            for col_idx in range(model.columnCount()):
                col_name = model.headerData(col_idx, Qt.Orientation.Horizontal)
                if col_name in default_widths:
                    width = default_widths[col_name]
                    header.resizeSection(col_idx, width)

                    if col_name in [
                        "100",
                        "50",
                        "Misses",
                        "Rank",
                        "PP",
                        "Accuracy",
                        "weight_%",
                        "weight_PP",
                    ]:
                        header.setSectionResizeMode(
                            col_idx, QHeaderView.ResizeMode.Fixed
                        )

            beatmap_col_idx = -1
            for col_idx in range(model.columnCount()):
                col_name = model.headerData(col_idx, Qt.Orientation.Horizontal)
                if col_name == "Beatmap":
                    beatmap_col_idx = col_idx
                    break

            if beatmap_col_idx >= 0:
                header.setSectionResizeMode(
                    beatmap_col_idx, QHeaderView.ResizeMode.Stretch
                )
            else:
                header.setStretchLastSection(True)

        except Exception as e:
            logger.error(f"Error setting column widths: {str(e)}")

            try:
                header.setStretchLastSection(True)
            except Exception as e:
                logger.debug(f"Error setting header stretch: {e}")

    def calculate_lost_scores_stats(self, data_df):
        try:
            if data_df.empty:
                self.stats_data["lost_scores"] = {}
                return

            total_scores = len(data_df)
            self.stats_data["lost_scores"]["total"] = total_scores

            if "PP" in data_df.columns and "Beatmap ID" in data_df.columns:
                try:
                    parsed_top_path = get_resource_path(
                        os.path.join("csv", "parsed_top.csv")
                    )
                    if os.path.exists(parsed_top_path):
                        top_df = pd.read_csv(parsed_top_path)

                        top_df = top_df[
                            ~top_df.iloc[:, 0]
                            .astype(str)
                            .str.contains("Sum|Overall|Difference", na=False)
                        ]

                        top_pp_dict = {}
                        for _, row in top_df.iterrows():
                            if (
                                "Beatmap ID" in top_df.columns
                                and "PP" in top_df.columns
                            ):
                                beatmap_id = row["Beatmap ID"]
                                pp = row["PP"]
                                if pd.notna(beatmap_id) and pd.notna(pp):
                                    top_pp_dict[str(int(beatmap_id))] = float(pp)

                        pp_diffs = []
                        for _, row in data_df.iterrows():
                            beatmap_id = (
                                str(int(row["Beatmap ID"]))
                                if pd.notna(row["Beatmap ID"])
                                else None
                            )
                            lost_pp = float(row["PP"]) if pd.notna(row["PP"]) else 0

                            if beatmap_id in top_pp_dict:
                                current_pp = top_pp_dict[beatmap_id]
                                pp_diff = lost_pp - current_pp
                                pp_diffs.append(pp_diff)

                        if pp_diffs:
                            avg_pp_diff = sum(pp_diffs) / len(pp_diffs)
                            self.stats_data["lost_scores"]["avg_pp_lost"] = avg_pp_diff
                        else:
                            avg_pp = data_df["PP"].astype(float).mean()
                            self.stats_data["lost_scores"]["avg_pp"] = avg_pp
                    else:
                        avg_pp = data_df["PP"].astype(float).mean()
                        self.stats_data["lost_scores"]["avg_pp"] = avg_pp
                except Exception as e:
                    logger.error(f"Error calculating AVG PP LOST: {e}")
        except Exception as e:
            logger.error(f"Error calculating lost scores stats: {str(e)}")
            self.stats_data["lost_scores"] = {}

    def update_stats_panel(self, tab_index):
        try:
            if not hasattr(self, "stats_panel") or not self.stats_panel:
                logger.error("stats_panel attribute missing")
                return

            if not hasattr(self, "stats_panel_layout") or not self.stats_panel_layout:
                logger.warning(
                    "stats_panel_layout attribute missing, getting from panel"
                )
                layout = self.stats_panel.layout()
                if layout:
                    self.stats_panel_layout = layout
                else:
                    logger.error("stats_panel has no layout")
                    return

            if not hasattr(self, "stats_data"):
                logger.warning("stats_data attribute missing, creating it")
                self.stats_data = {
                    "lost_scores": {},
                    "parsed_top": {},
                    "top_with_lost": {},
                }

            self.clear_stats_panel()

            if tab_index == 0:
                self.update_lost_scores_stats_panel()
            elif tab_index == 1:
                self.update_online_top_stats_panel()
            elif tab_index == 2:
                self.update_potential_top_stats_panel()
        except Exception as e:
            logger.error(f"Error updating stats panel: {e}")
            try:
                error_label = QLabel(f"Error: {str(e)}")
                error_label.setProperty("class", "StatsLabel")

                if hasattr(self, "stats_panel_layout") and self.stats_panel_layout:
                    self.stats_panel_layout.addWidget(error_label)
                    self.stats_panel_layout.addStretch()
                elif hasattr(self, "stats_panel") and self.stats_panel.layout():
                    self.stats_panel.layout().addWidget(error_label)
                    self.stats_panel.layout().addStretch()
            except Exception as inner_e:
                logger.error(f"Error displaying error message: {inner_e}")

    def clear_stats_panel(self):
        try:
            layout = None

            if hasattr(self, "stats_panel_layout") and self.stats_panel_layout:
                layout = self.stats_panel_layout

            elif hasattr(self, "stats_panel") and self.stats_panel.layout():
                layout = self.stats_panel.layout()

            if not layout:
                logger.error("Cannot clear stats panel: layout not found")
                return

            for i in reversed(range(layout.count())):
                item = layout.itemAt(i)
                if item and item.widget():
                    item.widget().deleteLater()
                elif item and item.spacerItem():
                    layout.removeItem(item)
        except Exception as e:
            logger.error(f"Error clearing stats panel: {e}")

    def update_lost_scores_stats_panel(self):
        if not self.stats_data["lost_scores"]:
            label = QLabel("No statistics available")
            label.setProperty("class", "StatsLabel")
            self.stats_panel.layout().addWidget(label)
            self.stats_panel.layout().addStretch()
            return

        try:
            total_scores = self.stats_data["lost_scores"].get("total", 0)
            total_scores = self.stats_data["lost_scores"].get("total", 0)
            scores_label = QLabel(f"TOTAL: {total_scores}")
            scores_label.setProperty("class", "StatsLabel Bold")
            self.stats_panel_layout.addWidget(scores_label)

            if "avg_pp_lost" in self.stats_data["lost_scores"]:
                avg_pp_diff = self.stats_data["lost_scores"]["avg_pp_lost"]
                pp_label = QLabel(f"AVG PP LOST: {avg_pp_diff:.2f}")
            elif "avg_pp" in self.stats_data["lost_scores"]:
                avg_pp = self.stats_data["lost_scores"]["avg_pp"]
                pp_label = QLabel(f"AVG PP: {avg_pp:.2f}")
            else:
                pp_label = QLabel("AVG PP: N/A")

            pp_label.setProperty("class", "StatsLabel")
            self.stats_panel_layout.addWidget(pp_label)

            self.stats_panel_layout.addStretch()
        except Exception as e:
            logger.error(f"Error updating lost scores stats panel: {str(e)}")
            error_label = QLabel(f"Error: {str(e)}")
            error_label.setProperty("class", "StatsLabel")
            self.stats_panel_layout.addWidget(error_label)
            self.stats_panel_layout.addStretch()

    def update_online_top_stats_panel(self):
        if not self.stats_data["parsed_top"]:
            label = QLabel("No statistics available")
            label.setProperty("class", "StatsLabel")
            self.stats_panel_layout.addWidget(label)
            self.stats_panel_layout.addStretch()
            return

        try:
            overall_pp = self.stats_data["parsed_top"].get("Overall PP", "N/A")
            pp_label = QLabel(f"Overall PP: {overall_pp}")
            pp_label.setProperty("class", "StatsLabel")
            self.stats_panel_layout.addWidget(pp_label)

            overall_acc = self.stats_data["parsed_top"].get("Overall Accuracy", "N/A")
            acc_label = QLabel(f"Overall Accuracy: {overall_acc}")
            acc_label.setProperty("class", "StatsLabel")
            self.stats_panel_layout.addWidget(acc_label)

            self.stats_panel_layout.addStretch()
        except Exception as e:
            logger.error(f"Error updating online top stats panel: {str(e)}")
            error_label = QLabel(f"Error: {str(e)}")
            error_label.setProperty("class", "StatsLabel")
            self.stats_panel_layout.addWidget(error_label)
            self.stats_panel_layout.addStretch()

    def update_potential_top_stats_panel(self):
        if not self.stats_data["top_with_lost"] and not self.stats_data["parsed_top"]:
            label = QLabel("No statistics available")
            label.setProperty("class", "StatsLabel")
            self.stats_panel_layout.addWidget(label)
            self.stats_panel_layout.addStretch()
            return

        try:
            current_pp = self.stats_data["parsed_top"].get("Overall PP", "N/A")
            current_pp_label = QLabel(f"Current PP: {current_pp}")
            current_pp_label.setProperty("class", "StatsLabel Bold")
            self.stats_panel_layout.addWidget(current_pp_label)

            potential_pp = self.stats_data["top_with_lost"].get(
                "Overall Potential PP", "N/A"
            )
            potential_pp_label = QLabel(f"Potential PP: {potential_pp}")
            potential_pp_label.setProperty("class", "StatsLabel Bold")

            self.stats_panel_layout.addWidget(potential_pp_label)

            current_acc = self.stats_data["parsed_top"].get("Overall Accuracy", "N/A")
            current_acc_label = QLabel(f"Current Accuracy: {current_acc}")
            current_acc_label.setProperty("class", "StatsLabel")
            self.stats_panel_layout.addWidget(current_acc_label)

            potential_acc = self.stats_data["top_with_lost"].get(
                "Overall Accuracy", "N/A"
            )
            potential_acc_label = QLabel(f"Potential Accuracy: {potential_acc}")
            potential_acc_label.setProperty("class", "StatsLabel")
            self.stats_panel_layout.addWidget(potential_acc_label)

            delta_pp = self.stats_data["top_with_lost"].get("Difference", "N/A")
            try:
                diff_num = float(delta_pp)
                if diff_num > 0:
                    delta_pp = f"+{delta_pp}"
                    delta_pp_label = QLabel(f"Δ PP: {delta_pp}")
                    delta_pp_label.setProperty("class", "StatsLabel PositiveValue")
                elif diff_num < 0:
                    delta_pp_label = QLabel(f"Δ PP: {delta_pp}")
                    delta_pp_label.setProperty("class", "StatsLabel NegativeValue")
                else:
                    delta_pp_label = QLabel(f"Δ PP: {delta_pp}")
                    delta_pp_label.setProperty("class", "StatsLabel")
            except ValueError:
                delta_pp_label = QLabel(f"Δ PP: {delta_pp}")
                delta_pp_label.setProperty("class", "StatsLabel")

            self.stats_panel_layout.addWidget(delta_pp_label)

                            
            delta_acc = self.stats_data["top_with_lost"].get(
                "Δ Overall Accuracy", "N/A"
            )
            delta_acc_label = QLabel(f"Δ Accuracy: {delta_acc}")

                                                                 
            if isinstance(delta_acc, str):
                if delta_acc.startswith("+"):
                    delta_acc_label.setProperty("class", "StatsLabel PositiveValue")
                elif delta_acc.startswith("-"):
                    delta_acc_label.setProperty("class", "StatsLabel NegativeValue")
                else:
                    delta_acc_label.setProperty("class", "StatsLabel")
            else:
                delta_acc_label.setProperty("class", "StatsLabel")

            self.stats_panel_layout.addWidget(delta_acc_label)

            self.stats_panel_layout.addStretch()
        except Exception as e:
            logger.error(f"Error updating potential top stats panel: {str(e)}")
            error_label = QLabel(f"Error: {str(e)}")
            error_label.setProperty("class", "StatsLabel")
            self.stats_panel_layout.addWidget(error_label)
            self.stats_panel_layout.addStretch()

    def focus_search(self):
        self.search_input.setFocus()
        self.search_input.selectAll()

    def perform_search(self):
        search_text = self.search_input.text().strip().lower()
        if not search_text:
            self.search_count_label.setText("")
            self.prev_result_button.setVisible(False)
            self.next_result_button.setVisible(False)
            self.search_results = []
            self.current_result_index = -1
            return

        current_tab_index = self.tab_widget.currentIndex()
        if current_tab_index == 0:
            current_table = self.lost_scores_view
        elif current_tab_index == 1:
            current_table = self.parsed_top_view
        else:
            current_table = self.top_with_lost_view

        model = current_table.model()
        if not model:
            return

        current_table.clearSelection()

        self.search_results = []
        for row in range(model.rowCount()):
            for col in range(model.columnCount()):
                idx = model.index(row, col)
                cell_value = model.data(idx)
                if cell_value and search_text in str(cell_value).lower():
                    self.search_results.append((row, col))

        self.update_search_ui()

        if self.search_results:
            self.current_result_index = 0
            self.highlight_current_result(current_table)
        else:
            QMessageBox.information(
                self, "Search Results", f"No matches found for '{search_text}'"
            )
            self.prev_result_button.setVisible(False)
            self.next_result_button.setVisible(False)

    def show_table_context_menu(self, table_view, position):
        menu = QMenu()

        copy_action = menu.addAction("Copy")
        copy_action.triggered.connect(lambda: self.copy_selected_cells(table_view))

        copy_action.setEnabled(len(table_view.selectedIndexes()) > 0)

        menu.addSeparator()

        select_all_action = menu.addAction("Select All")
        select_all_action.triggered.connect(table_view.selectAll)

        global_pos = table_view.mapToGlobal(position)
        menu.exec(QPoint(global_pos.x() + 24, global_pos.y() + 32))

    def copy_selected_cells(self, table_view, show_tooltip=False):
        selected = table_view.selectedIndexes()
        if not selected:
            return

        rows = set(index.row() for index in selected)
        cols = set(index.column() for index in selected)

        min_row = min(rows)
        max_row = max(rows)
        min_col = min(cols)
        max_col = max(cols)

        table_text = []
        for row in range(min_row, max_row + 1):
            row_data = []
            for col in range(min_col, max_col + 1):
                for index in selected:
                    if index.row() == row and index.column() == col:
                        row_data.append(str(table_view.model().data(index)))
                        break
                else:
                    row_data.append("")
            table_text.append("\t".join(row_data))

        clipboard_text = "\n".join(table_text)
        try:
            import pyperclip

            pyperclip.copy(clipboard_text)
        except ImportError:
            clipboard = QApplication.clipboard()
            clipboard.setText(clipboard_text)

        if show_tooltip:
            QToolTip.showText(
                table_view.mapToGlobal(QPoint(0, 0)),
                f"Copied {len(selected)} cell(s) to clipboard",
                table_view,
                QRect(),
                2000,
            )

    def show_context_menu(self, widget, position):
        menu = QMenu()

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
            menu.exec(
                QPoint(
                    widget.mapToGlobal(position).x() + 5,
                    widget.mapToGlobal(position).y() + 5,
                )
            )

    def update_search_ui(self):
        result_count = len(self.search_results)

        if result_count > 0:
            current_pos = (
                self.current_result_index + 1 if self.current_result_index >= 0 else 0
            )
            self.search_count_label.setText(f"{current_pos}/{result_count}")

            self.prev_result_button.setVisible(result_count > 0)
            self.next_result_button.setVisible(result_count > 0)
        else:
            self.search_count_label.setText("")
            self.prev_result_button.setVisible(False)
            self.next_result_button.setVisible(False)

    def highlight_current_result(self, table_view):
        if not self.search_results or self.current_result_index < 0:
            return

        current_row, current_col = self.search_results[self.current_result_index]
        model = table_view.model()
        current_idx = model.index(current_row, current_col)

        table_view.selectRow(current_row)

        table_view.scrollTo(current_idx, QTableView.ScrollHint.PositionAtCenter)

        self.update_search_ui()

    def go_to_next_result(self):
        if not self.search_results:
            return

        current_tab_index = self.tab_widget.currentIndex()
        if current_tab_index == 0:
            current_table = self.lost_scores_view
        elif current_tab_index == 1:
            current_table = self.parsed_top_view
        else:
            current_table = self.top_with_lost_view

        self.current_result_index = (self.current_result_index + 1) % len(
            self.search_results
        )
        self.highlight_current_result(current_table)

    def go_to_previous_result(self):
        if not self.search_results:
            return

        current_tab_index = self.tab_widget.currentIndex()
        if current_tab_index == 0:
            current_table = self.lost_scores_view
        elif current_tab_index == 1:
            current_table = self.parsed_top_view
        else:
            current_table = self.top_with_lost_view

        self.current_result_index = (self.current_result_index - 1) % len(
            self.search_results
        )
        self.highlight_current_result(current_table)


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("osu! Lost Scores Analyzer")
        self.setGeometry(100, 100, 650, 500)
        self.setFixedSize(650, 500)
        self.setObjectName("mainWindow")

        self.scan_completed = threading.Event()
        self.top_completed = threading.Event()
        self.img_completed = threading.Event()
        self.has_error = False
        self.overall_progress = 0
        self.current_task = "Ready to start"

        self.load_icons()

        self.initUI()
        self.load_background()

        self.config = {}
        self.load_config()

        if "osu_path" in self.config and self.config["osu_path"]:
            self.game_entry.setText(self.config["osu_path"])
        if "username" in self.config and self.config["username"]:
            self.profile_entry.setText(self.config["username"])
        if "scores_count" in self.config and self.config["scores_count"]:
            self.scores_count_entry.setText(str(self.config["scores_count"]))

        self.enable_results_button()

        self.threadpool = QThreadPool()
        self.threadpool.setMaxThreadCount(GUI_THREAD_POOL_SIZE)
        logger.info(f"Max threads in pool: {self.threadpool.maxThreadCount()}")

        self._try_auto_detect_osu_path()

        from osu_api import get_keys_from_keyring

        client_id, client_secret = get_keys_from_keyring()

        if not client_id or not client_secret:
            QtCore.QTimer.singleShot(500, self.show_first_run_api_dialog)

    def show_first_run_api_dialog(self):
        QMessageBox.information(
            self,
            "API Keys Required",
            "Welcome to osu! Lost Scores Analyzer!\n\n"
            "To use this application, you need to provide osu! API keys.\n"
            "Please enter your API keys in the next dialog.",
        )
        self.open_api_dialog()

    def enable_results_button(self):
        try:
            csv_files = [
                get_resource_path(os.path.join("csv", "lost_scores.csv")),
                get_resource_path(os.path.join("csv", "parsed_top.csv")),
                get_resource_path(os.path.join("csv", "top_with_lost.csv")),
            ]

            has_data = False
            for file_path in csv_files:
                if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                    has_data = True
                    break

            self.results_button.setEnabled(has_data)

            if has_data:
                logger.debug("Results data found. 'See Full Results' button enabled.")
            else:
                logger.debug(
                    "No results data found. 'See Full Results' button disabled."
                )

        except Exception as e:
            logger.error(f"Error checking for results files: {e}")

            self.results_button.setEnabled(False)

    def ensure_csv_files_exist(self):
        csv_dir = get_resource_path("csv")
        os.makedirs(csv_dir, exist_ok=True)

        lost_scores_path = os.path.join(csv_dir, "lost_scores.csv")
        if not os.path.exists(lost_scores_path):
            try:
                with open(lost_scores_path, "w", encoding="utf-8") as f:
                    f.write(
                        "PP,Beatmap ID,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,Rank\n"
                    )
                self.append_log("Created empty file lost_scores.csv", False)
            except Exception as e:
                self.append_log(f"Error creating lost_scores.csv: {e}", False)

        parsed_top_path = os.path.join(csv_dir, "parsed_top.csv")
        if not os.path.exists(parsed_top_path):
            try:
                with open(parsed_top_path, "w", encoding="utf-8") as f:
                    f.write(
                        "PP,Beatmap ID,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,weight_%,weight_PP,Score ID,Rank\n"
                    )
                self.append_log("Created empty file parsed_top.csv", False)
            except Exception as e:
                self.append_log(f"Error creating parsed_top.csv: {e}", False)

        top_with_lost_path = os.path.join(csv_dir, "top_with_lost.csv")
        if not os.path.exists(top_with_lost_path):
            try:
                with open(top_with_lost_path, "w", encoding="utf-8") as f:
                    f.write(
                        "PP,Beatmap ID,Status,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,Rank,weight_%,weight_PP,Score ID\n"
                    )
                self.append_log("Created empty file top_with_lost.csv", False)
            except Exception as e:
                self.append_log(f"Error creating top_with_lost.csv: {e}", False)

    def load_config(self):
        self.config = {}
        try:
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    self.config = json.load(f)
                logger.info(
                    "Configuration loaded from %s", mask_path_for_log(CONFIG_PATH)
                )

        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            self.config = {}

        if "include_unranked" in self.config:
            self.include_unranked_checkbox.setChecked(self.config["include_unranked"])
        if "show_lost" in self.config:
            self.show_lost_checkbox.setChecked(self.config["show_lost"])
        if "clean_scan" in self.config:
            self.clean_scan_checkbox.setChecked(self.config["clean_scan"])

    def save_config(self):
        try:
            self.config["osu_path"] = self.game_entry.text().strip()
            self.config["username"] = self.profile_entry.text().strip()

            scores_count = self.scores_count_entry.text().strip()
            if scores_count:
                try:
                    self.config["scores_count"] = int(scores_count)
                except ValueError:
                    self.config["scores_count"] = 10

            self.config["include_unranked"] = self.include_unranked_checkbox.isChecked()
            self.config["show_lost"] = self.show_lost_checkbox.isChecked()
            self.config["clean_scan"] = self.clean_scan_checkbox.isChecked()

            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(self.config, f, indent=4)

            logger.info(
                "Configuration saved to %s",
                mask_path_for_log(os.path.normpath(CONFIG_PATH)),
            )
        except Exception as e:
            logger.error("Error saving configuration: %s", e)

    def closeEvent(self, event):
        self.save_config()

        try:
            from database import db_close

            db_close()
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

        event.accept()

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
                    logger.warning(f"Icon file not found: {mask_path_for_log(path)}")
                    self.icons[name][state] = QIcon()

    def load_background(self):
        BACKGROUND_IMAGE_PATH = get_resource_path(
            os.path.join("assets", "background", "bg.png")
        )
        self.background_pixmap = None
        if os.path.exists(BACKGROUND_IMAGE_PATH):
            try:
                self.background_pixmap = QPixmap(BACKGROUND_IMAGE_PATH)
                if self.background_pixmap.isNull():
                    self.background_pixmap = None
                    logger.warning(
                        "Failed to load background: %s",
                        mask_path_for_log(os.path.normpath(BACKGROUND_IMAGE_PATH)),
                    )
                else:
                    logger.info("Background image loaded.")
            except Exception as e:
                logger.error("Error loading background: %s", e)
                self.background_pixmap = None
        else:
            logger.warning(
                "Background file not found: %s",
                mask_path_for_log(os.path.normpath(BACKGROUND_IMAGE_PATH)),
            )

    def paintEvent(self, event):
        painter = QPainter(self)
        if hasattr(self, "background_pixmap") and self.background_pixmap:
            scaled_pixmap = self.background_pixmap.scaled(
                self.size(),
                Qt.AspectRatioMode.IgnoreAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
            painter.drawPixmap(self.rect(), scaled_pixmap)
        else:
            painter.fillRect(self.rect(), QColor("#251a37"))
        painter.end()

    def initUI(self):
        window_height = 835
        self.setGeometry(100, 100, 650, window_height)
        self.setFixedSize(650, window_height)

        self.setLayout(None)

        self.title_label = QLabel(self)
        self.title_label.setGeometry(50, 20, 550, 50)
        self.title_label.setObjectName("TitleLabel")
        self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.title_label.setText(
            '<span style="color: #ee4bbd;">osu!</span><span style="color: white;"> Lost Scores Analyzer</span> 🍋'
        )
        self.title_label.setTextFormat(Qt.TextFormat.RichText)

        dir_label = QLabel("osu! Game Directory", self)
        dir_label.setGeometry(50, 90, 550, 30)
        dir_label.setObjectName("dirLabel")

        dir_container = QFrame(self)
        dir_container.setGeometry(50, 125, 550, 40)
        dir_container.setObjectName("dirContainer")

        self.game_entry = QLineEdit(dir_container)
        self.game_entry.setGeometry(10, 0, 500, 40)
        self.game_entry.setObjectName("gameEntry")
        self.game_entry.setPlaceholderText("Path to your osu! installation folder...")

        self.browse_button = FolderButton(
            self.icons.get("folder", {}).get("normal"),
            self.icons.get("folder", {}).get("hover"),
            dir_container,
        )

        self.browse_button.setGeometry(510, 5, 30, 30)
        self.browse_button.clicked.connect(self.browse_directory)

        url_label = QLabel("Username (or ID / URL)", self)
        url_label.setGeometry(50, 180, 550, 30)
        url_label.setObjectName("urlLabel")

        self.profile_entry = QLineEdit(self)
        self.profile_entry.setGeometry(50, 215, 550, 40)
        self.profile_entry.setObjectName("profileEntry")
        self.profile_entry.setPlaceholderText("e.g., https://osu.ppy.sh/users/2")

        scores_label = QLabel("Number of scores to display", self)
        scores_label.setGeometry(50, 270, 550, 30)
        scores_label.setObjectName("scoresLabel")

        self.scores_count_entry = QLineEdit(self)

        self.scores_count_entry.setGeometry(50, 305, 350, 40)
        self.scores_count_entry.setObjectName("scoresCountEntry")
        self.api_button = QPushButton("API Keys", self)
        self.api_button.setGeometry(410, 305, 190, 40)
        self.api_button.setObjectName("apiButton")
        self.api_button.clicked.connect(self.open_api_dialog)
        checkbox_y = 365

        self.include_unranked_checkbox = QCheckBox(
            "Include unranked/loved beatmaps", self
        )
        self.include_unranked_checkbox.setGeometry(50, checkbox_y, 550, 25)
        self.include_unranked_checkbox.setObjectName("includeUnrankedCheckbox")

        self.show_lost_checkbox = QCheckBox("Show at least one lost score", self)
        self.show_lost_checkbox.setGeometry(50, checkbox_y + 35, 550, 25)
        self.show_lost_checkbox.setObjectName("showLostCheckbox")

        self.clean_scan_checkbox = QCheckBox("Perform clean scan (reset cache)", self)
        self.clean_scan_checkbox.setGeometry(50, checkbox_y + 70, 550, 25)
        self.clean_scan_checkbox.setObjectName("cleanScanCheckbox")

        self.scores_count_entry.setPlaceholderText("For example, 10")

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

        btn_y = 470
        self.btn_all = QPushButton("Start Scan", self)
        self.btn_all.setGeometry(50, btn_y, 550, 50)
        self.btn_all.setObjectName("btnAll")
        self.btn_all.clicked.connect(self.start_all_processes)

        self.progress_bar = AnimatedProgressBar(self)
        self.progress_bar.setGeometry(50, btn_y + 65, 550, 20)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setObjectName("progressBar")

        self.status_label = QLabel(self.current_task, self)
        self.status_label.setGeometry(50, btn_y + 90, 550, 25)
        self.status_label.setObjectName("StatusLabel")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        log_label = QLabel("Log", self)
        log_label.setGeometry(50, btn_y + 130, 550, 25)
        log_label.setObjectName("logLabel")

        log_container = QFrame(self)
        log_container.setGeometry(50, btn_y + 160, 550, 120)
        log_container.setObjectName("LogContainer")
        log_container.setFrameShape(QFrame.Shape.NoFrame)
        log_container.setAutoFillBackground(True)

        log_layout = QVBoxLayout(log_container)
        log_layout.setContentsMargins(5, 5, 5, 5)

        self.log_textbox = QTextEdit(log_container)
        self.log_textbox.setObjectName("logTextbox")
        self.log_textbox.setReadOnly(True)

        log_layout.addWidget(self.log_textbox)

        self.results_button = QPushButton("See Full Results", self)
        self.results_button.setGeometry(50, btn_y + 290, 550, 40)
        self.results_button.setObjectName("resultsButton")
        self.results_button.clicked.connect(self.show_results_window)
        self.results_button.setEnabled(False)

        self.log_textbox.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.log_textbox.customContextMenuRequested.connect(
            partial(self.show_context_menu, self.log_textbox)
        )

        self.game_entry.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.game_entry.customContextMenuRequested.connect(
            partial(self.show_context_menu, self.game_entry)
        )

        self.profile_entry.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.profile_entry.customContextMenuRequested.connect(
            partial(self.show_context_menu, self.profile_entry)
        )

        self.scores_count_entry.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu
        )
        self.scores_count_entry.customContextMenuRequested.connect(
            partial(self.show_context_menu, self.scores_count_entry)
        )

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

                                                                         
                                                       
                gui_log_messages = [
                    "button enabled",
                    "button disabled",
                    "created empty file",
                    "found results",
                    "opened",
                    "button",
                    "selected",
                    "loaded",
                ]

                if any(marker in message.lower() for marker in gui_log_messages):
                    logger.debug(message)
                else:
                    logger.info(message)

        except Exception as e:
            logger.error(
                "Exception inside append_log when processing message '%s': %s",
                message,
                e,
            )

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
        logger.info("Background task completed.")

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
        folder = QFileDialog.getExistingDirectory(
            self, "Select osu! Game Directory", ""
        )
        if folder:
            self.game_entry.setText(folder.replace("/", os.sep))
            self.append_log(f"Selected folder: {mask_path_for_log(folder)}", False)

            self.save_config()

    def start_all_processes(self):
        game_dir = self.game_entry.text().strip()
        user_input = self.profile_entry.text().strip()

        if not game_dir or not user_input:
            QMessageBox.warning(
                self,
                "Error",
                "Please specify osu! folder and profile input (URL/ID/Username).",
            )
            return

        if not os.path.isdir(game_dir):
            QMessageBox.warning(
                self, "Error", f"Specified directory doesn't exist: {game_dir}"
            )
            return

        songs_dir = os.path.join(game_dir, "Songs")
        replays_dir = os.path.join(game_dir, "Data", "r")

        if not os.path.isdir(songs_dir):
            QMessageBox.warning(
                self, "Error", f"Songs directory not found: {songs_dir}"
            )
            return

        if not os.path.isdir(replays_dir):
            QMessageBox.warning(
                self, "Error", f"Replays directory not found: {replays_dir}"
            )
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

                folders_to_clean = [
                    get_resource_path("cache"),
                    get_resource_path("maps"),
                    get_resource_path("results"),
                    get_resource_path("csv"),
                    get_resource_path("assets/images"),
                ]
                logger.debug(
                    f"Folders to clean absolute paths: {mask_path_for_log(folders_to_clean)}"
                )

                for folder in folders_to_clean:
                    abs_folder_path = os.path.abspath(folder)
                    if os.path.exists(abs_folder_path):
                        self.append_log(
                            f"Deleting folder: {mask_path_for_log(abs_folder_path)}",
                            False,
                        )
                        try:
                            shutil.rmtree(abs_folder_path)
                        except OSError as e:
                            logger.error(
                                f"Error removing directory {abs_folder_path}: {e}"
                            )

                            if os.path.isdir(abs_folder_path):
                                for item in os.listdir(abs_folder_path):
                                    item_path = os.path.join(abs_folder_path, item)
                                    try:
                                        if os.path.isfile(item_path) or os.path.islink(
                                            item_path
                                        ):
                                            os.unlink(item_path)
                                        elif os.path.isdir(item_path):
                                            shutil.rmtree(item_path)
                                    except Exception as ex_inner:
                                        logger.error(
                                            f"Failed to delete item {mask_path_for_log(item_path)}: {ex_inner}"
                                        )

                                        raise e from ex_inner
                            else:
                                raise

                        if not os.path.exists(abs_folder_path):
                            os.makedirs(abs_folder_path, exist_ok=True)
                            self.append_log(
                                f"Folder recreated: {abs_folder_path}", False
                            )
                    else:
                        os.makedirs(abs_folder_path, exist_ok=True)
                        self.append_log(
                            f"Folder created (did not exist): {abs_folder_path}", False
                        )

                self.append_log(
                    "Re-initializing database connection after cleaning...", False
                )
                db_init()
                logger.info("Database re-initialized after cache cleaning.")

                self.append_log("Resetting in-memory caches...", False)
                reset_in_memory_caches()

                self.ensure_csv_files_exist()
                self.append_log("Cache clearing completed successfully", False)

            except (FileNotFoundError, PermissionError, OSError) as e:
                self.append_log(f"Error clearing cache: {e}", False)

                try:
                    self.append_log(
                        "Attempting DB re-initialization after cache error...", False
                    )
                    db_init()
                except Exception as db_err:
                    self.append_log(
                        f"Failed to re-initialize DB after cache error: {db_err}", False
                    )

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
        self.results_button.setEnabled(False)

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
                self.action_scan, "click", QtCore.Qt.ConnectionType.QueuedConnection
            )

            max_wait_time = 3600
            wait_start = time.time()

            while not self.scan_completed.is_set():
                if time.time() - wait_start > max_wait_time:
                    logger.error("Maximum wait time exceeded for replay scanning")
                    QtCore.QMetaObject.invokeMethod(
                        self,
                        "task_error",
                        QtCore.Qt.ConnectionType.QueuedConnection,
                        QtCore.Q_ARG(str, "Scan timeout exceeded"),
                    )
                    return

                time.sleep(0.1)

            if self.has_error:
                logger.error("Scanning completed with error, aborting sequence")
                return

            QtCore.QMetaObject.invokeMethod(
                self.action_top, "click", QtCore.Qt.ConnectionType.QueuedConnection
            )

            wait_start = time.time()

            while not self.top_completed.is_set():
                if time.time() - wait_start > max_wait_time:
                    logger.error(
                        "Maximum wait time exceeded for potential top creation"
                    )
                    QtCore.QMetaObject.invokeMethod(
                        self,
                        "task_error",
                        QtCore.Qt.ConnectionType.QueuedConnection,
                        QtCore.Q_ARG(str, "Top creation timeout exceeded"),
                    )
                    return
                time.sleep(0.1)

            if self.has_error:
                logger.error("Top creation completed with error, aborting sequence")
                return

            QtCore.QMetaObject.invokeMethod(
                self.action_img, "click", QtCore.Qt.ConnectionType.QueuedConnection
            )

            wait_start = time.time()

            while not self.img_completed.is_set():
                if time.time() - wait_start > max_wait_time:
                    logger.error("Maximum wait time exceeded for image creation")
                    QtCore.QMetaObject.invokeMethod(
                        self,
                        "task_error",
                        QtCore.Qt.ConnectionType.QueuedConnection,
                        QtCore.Q_ARG(str, "Image creation timeout exceeded"),
                    )
                    return
                time.sleep(0.1)

            if not self.has_error:
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "all_completed_successfully",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                )
        except Exception as e:
            logger.error(f"Sequential launch error: {e}")
            QtCore.QMetaObject.invokeMethod(
                self,
                "task_error",
                QtCore.Qt.ConnectionType.QueuedConnection,
                QtCore.Q_ARG(str, f"Sequential launch error: {e}"),
            )
        finally:
            QtCore.QMetaObject.invokeMethod(
                self, "enable_all_button", QtCore.Qt.ConnectionType.QueuedConnection
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

        self.enable_results_button()

        if os.path.exists(results_path) and os.path.isdir(results_path):
            self.append_log(
                f"Opening results folder: {mask_path_for_log(results_path)}", False
            )
            self.open_folder(results_path)
        else:
            self.append_log(
                f"Results folder not found: {mask_path_for_log(results_path)}", False
            )

        QMessageBox.information(
            self,
            "Done",
            "Analysis completed! You can find results in the 'results' folder.",
        )
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
            QMessageBox.warning(
                self,
                "Error",
                "Please specify osu! folder and profile input (URL/ID/Username).",
            )
            self.scan_completed.set()
            return

        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None:
            self.scan_completed.set()
            return

        self.append_log("Starting replay scanning...", False)
        self.progress_bar.setValue(0)

        include_unranked = self.include_unranked_checkbox.isChecked()
        worker = Worker(
            scan_replays,
            game_dir,
            identifier,
            lookup_key,
            include_unranked=include_unranked,
        )
        worker.signals.progress.connect(self.update_progress_bar)
        worker.signals.log.connect(self.append_log)
        worker.signals.finished.connect(self.task_finished)
        worker.signals.error.connect(self.task_error)
        self.threadpool.start(worker)

    def start_top(self):
        game_dir = self.game_entry.text().strip()
        user_input = self.profile_entry.text().strip()
        if not game_dir or not user_input:
            QMessageBox.warning(
                self,
                "Error",
                "Please specify osu! folder and profile input (URL/ID/Username).",
            )
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
            QMessageBox.warning(
                self, "Error", "Please specify profile input (URL/ID/Username)."
            )
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
                    QtCore.Q_ARG(int, 100),
                )

                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_task",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, "Getting API token..."),
                )

                token = img_mod.get_token_osu()
                if not token:
                    raise ValueError("Failed to get osu! API token!")

                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_progress_bar",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(int, 70),
                    QtCore.Q_ARG(int, 100),
                )

                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_task",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, "Getting user data..."),
                )

                user_data = img_mod.get_user_osu(user_id_or_name, key_type, token)
                if not user_data:
                    error_msg = f"Failed to get user data '{user_id_or_name}' (type: {key_type})."
                    QtCore.QMetaObject.invokeMethod(
                        self,
                        "img_error",
                        QtCore.Qt.ConnectionType.QueuedConnection,
                        QtCore.Q_ARG(str, error_msg),
                    )
                    return

                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_progress_bar",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(int, 75),
                    QtCore.Q_ARG(int, 100),
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
                    QtCore.Q_ARG(bool, False),
                )

                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_task",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, "Creating lost scores image..."),
                )

                img_mod.make_img_lost(
                    user_id=uid, user_name=uname, max_scores=num_scores
                )
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_progress_bar",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(int, 85),
                    QtCore.Q_ARG(int, 100),
                )

                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_task",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, "Creating potential top image..."),
                )

                img_mod.make_img_top(
                    user_id=uid,
                    user_name=uname,
                    max_scores=num_scores,
                    show_lost=show_lost_flag,
                )
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "update_progress_bar",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(int, 100),
                    QtCore.Q_ARG(int, 100),
                )

                QtCore.QMetaObject.invokeMethod(
                    self, "img_finished", QtCore.Qt.ConnectionType.QueuedConnection
                )

            except Exception as e:
                error_message = f"Error in image generation thread: {e}"
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "img_error",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, error_message),
                )

        threading.Thread(
            target=task,
            args=(identifier, lookup_key, scores_count, show_lost),
            daemon=True,
        ).start()

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
        QMessageBox.critical(
            self,
            "Error generating images",
            f"Failed to create images.\n{error_message}",
        )
        self.progress_bar.setValue(60)
        self.current_task = "Error generating images"
        self.status_label.setText(self.current_task)
        self.img_completed.set()

    def _parse_user_input(self, user_input):
        identifier = user_input
        lookup_key = "username"

        if user_input.startswith(("http://", "https://")):
            try:
                parts = user_input.strip("/").split("/")
                if len(parts) >= 2 and parts[-2] == "users":
                    identifier = parts[-1]
                elif len(parts) >= 1 and parts[-1].isdigit():
                    identifier = parts[-1]
                else:
                    raise IndexError("Failed to extract ID/username from URL")

            except IndexError:
                QMessageBox.warning(self, "Error", f"Invalid profile URL: {user_input}")
                return None, None

            if identifier.isdigit():
                lookup_key = "id"
            else:
                lookup_key = "username"

        elif user_input.isdigit():
            identifier = user_input
            lookup_key = "id"
        else:
            identifier = user_input
            lookup_key = "username"

        return identifier, lookup_key

    def show_context_menu(self, widget, position):
        menu = QMenu()

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

        elif isinstance(widget, QTextEdit):
            copy_action = menu.addAction("Copy")
            copy_action.triggered.connect(widget.copy)
            copy_action.setEnabled(widget.textCursor().hasSelection())

            menu.addSeparator()

            select_all_action = menu.addAction("Select All")
            select_all_action.triggered.connect(widget.selectAll)
            select_all_action.setEnabled(bool(widget.toPlainText()))

        if menu.actions():
            menu.exec(widget.mapToGlobal(position))

    def show_results_window(self):
        try:
            self.results_window = ResultsWindow(self)

            self.results_window.show()

            QApplication.processEvents()

            self.results_window.showMaximized()

        except Exception as e:
            logger.error(f"Error showing results window: {e}")
            QMessageBox.critical(
                self, "Error", f"Failed to open results window: {str(e)}"
            )

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
        if "osu_path" in self.config and self.config["osu_path"]:
            saved_path = self.config["osu_path"]
            if os.path.isdir(saved_path):
                self.game_entry.setText(saved_path.replace("/", os.sep))
                self.append_log(
                    f"Loaded path from configuration: {mask_path_for_log(saved_path)}",
                    False,
                )
                return

        potential_paths = []

        if platform.system() == "Windows":
            local_app_data = os.getenv("LOCALAPPDATA")
            if local_app_data:
                potential_paths.append(os.path.join(local_app_data, "osu!"))

            for drive in ["C:", "D:", "E:", "F:"]:
                try:
                    if os.path.exists(f"{drive}\\Users"):
                        for username in os.listdir(f"{drive}\\Users"):
                            user_appdata = (
                                f"{drive}\\Users\\{username}\\AppData\\Local\\osu!"
                            )
                            if os.path.isdir(user_appdata):
                                potential_paths.append(user_appdata)
                except Exception:
                    pass

        for path in potential_paths:
            if os.path.isdir(path):
                self.game_entry.setText(path.replace("/", os.sep))
                self.append_log(
                    f"osu! folder automatically found: {mask_path_for_log(path)}", False
                )

                self.config["osu_path"] = path
                self.save_config()
                return

        self.append_log(
            "osu! folder not found automatically. Please specify path manually.", False
        )

    def open_api_dialog(self):
        from osu_api import (
            get_keys_from_keyring,
            save_keys_to_keyring,
            delete_keys_from_keyring,
        )

        current_client_id, current_client_secret = get_keys_from_keyring()
        keys_existed_before_dialog = bool(current_client_id and current_client_secret)

        dialog = ApiDialog(
            self,
            current_client_id or "",
            current_client_secret or "",
            keys_currently_exist=keys_existed_before_dialog,
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
                        QMessageBox.StandardButton.No,
                    )

                    if reply == QMessageBox.StandardButton.Yes:
                        if delete_keys_from_keyring():
                            QMessageBox.information(
                                self,
                                "Success",
                                "API keys have been removed successfully.",
                            )
                        else:
                            QMessageBox.critical(
                                self, "Error", "Failed to remove API keys."
                            )
                else:
                    QMessageBox.warning(
                        self,
                        "Empty API Keys",
                        "API keys cannot be empty. Please enter valid Client ID and Client Secret.",
                    )
                return

            if not client_id or not client_secret:
                QMessageBox.warning(
                    self,
                    "Incomplete API Keys",
                    "Both Client ID and Client Secret are required.",
                )
                return

            if save_keys_to_keyring(client_id, client_secret):
                QMessageBox.information(self, "Success", "API keys saved successfully!")
            else:
                QMessageBox.critical(
                    self, "Error", "Failed to save API keys to system keyring."
                )


def create_gui():
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)

    font_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets", "fonts"
    )
    if os.path.isdir(font_path):
        font_db = QFontDatabase()
        fonts_loaded = 0
        for filename in os.listdir(font_path):
            if filename.lower().endswith((".ttf", ".otf")):
                font_id = font_db.addApplicationFont(os.path.join(font_path, filename))
                if font_id != -1:
                    fonts_loaded += 1
        if fonts_loaded > 0:
            logger.info(f"Loaded {fonts_loaded} local fonts")

    qss = load_qss()
    if qss:
        app.setStyleSheet(qss)
        logger.debug("QSS styles SUCCESSFULLY applied to QApplication.")

    else:
        logger.debug("QSS styles WERE NOT applied (content empty or loading error).")

    window = MainWindow()
    window.show()
    return window


if __name__ == "__main__":
    app = QApplication(sys.argv)

    font_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "assets", "fonts"
    )
    if os.path.isdir(font_path):
        font_db = QFontDatabase()
        for filename in os.listdir(font_path):
            if filename.lower().endswith((".ttf", ".otf")):
                font_db.addApplicationFont(os.path.join(font_path, filename))

    qss = load_qss()
    if qss:
        app.setStyleSheet(qss)
        logger.debug("QSS styles SUCCESSFULLY applied to QApplication (from __main__).")
    else:
        logger.debug("QSS styles WERE NOT applied (from __main__).")

    window = create_gui()
    sys.exit(app.exec())
