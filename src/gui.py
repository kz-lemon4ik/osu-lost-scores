
import json
import logging
import inspect
import os
import os.path
import platform
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from functools import partial

import pandas as pd
from PySide6 import QtCore, QtGui
from color_constants import (
    ACCENT_COLOR, TEXT_PRIMARY, TEXT_SECONDARY,
    ERROR_COLOR, SEPARATOR_COLOR, LINK_COLOR,
    QCOLOR_PRIMARY_BG, QCOLOR_SECONDARY_BG, QCOLOR_ACCENT, QCOLOR_TEXT_PRIMARY,
)
from PySide6.QtCore import (
    QAbstractTableModel,
    QEasingCurve,
    QModelIndex,
    QObject,
    QPoint,
    QPropertyAnimation,
    QRect,
    QRunnable,
    QSize,
    Qt,
    QThreadPool,
    Signal,
    Slot,
    QByteArray,
)
from PySide6.QtGui import (
    QFontDatabase,
    QIcon,
    QKeySequence,
    QPainter,
    QPixmap,
    QShortcut,
)
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSizePolicy,
    QStackedWidget,
    QTableView,
    QTabWidget,
    QTextEdit,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

import generate_image as img_mod
from analyzer import make_top, scan_replays
from app_config import (
    API_REQUESTS_PER_MINUTE,
    CACHE_DIR,
    CSV_DIR,
    DB_FILE,
    GUI_THREAD_POOL_SIZE,
    MAPS_DIR,
    RESULTS_DIR,
)
from database import db_close, db_init
from file_parser import file_parser
from osu_api import OsuApiClient
from path_utils import get_standard_dir, mask_path_for_log
from utils import (
    create_standard_edit_menu,
    load_summary_stats,
    get_delta_color,
)
from auth_manager import AuthManager, AuthMode
from oauth_browser import BrowserOAuthFlow

logger = logging.getLogger(__name__)

try:
    import pyperclip

    PYPERCLIP_AVAILABLE = True
except ImportError:
    pyperclip = None
    logger.warning("Module 'pyperclip' not found. Clipboard functions will be limited")
    PYPERCLIP_AVAILABLE = False

ICON_PATH = get_standard_dir("assets/images/icons")
FONT_PATH = get_standard_dir("assets/fonts")
BACKGROUND_FOLDER_PATH = get_standard_dir("assets/images/background")
BACKGROUND_IMAGE_PATH = get_standard_dir("assets/images/background/bg.png")
APP_ICON_PATH = get_standard_dir("assets/images/app_icon/icon.ico")
CONFIG_PATH = get_standard_dir("config/gui_config.json")
os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)

def load_qss():
    style_path = get_standard_dir("assets/styles/style.qss")
    logger.debug(
        "Attempting to load QSS from: %s",
        mask_path_for_log(os.path.normpath(style_path)),
    )
    try:
        with open(style_path, "r", encoding="utf-8") as f:
            qss_content = f.read()
        logger.debug("QSS file successfully read (%d bytes)", len(qss_content))
        return qss_content
    except Exception as e:
        logger.warning("ERROR loading QSS file: %s", e)
        return ""

# noinspection PyTypeChecker
def show_api_limit_warning():
    if 60 < API_REQUESTS_PER_MINUTE <= 1200:
        QMessageBox.warning(
            None,  # type: ignore
            "API Rate Limit Warning",
            f"High API request rate detected\n\nCurrent setting: {API_REQUESTS_PER_MINUTE} requests per minute\n\n"
            f"WARNING: peppy prohibits using more than 60 requests per minute.\n"
            f"Burst spikes up to 1200 requests per minute are possible, but proceed at your own risk.\n"
            f"It may result in API/website usage ban",
        )
    elif API_REQUESTS_PER_MINUTE > 1200:
        QMessageBox.critical(
            None,  # type: ignore
            "Excessive API Rate",
            f"Extremely high API request rate detected\n\nCurrent setting: {API_REQUESTS_PER_MINUTE} requests per minute\n\n"
            f"WARNING: This exceeds the maximum burst limit of 1200 requests per minute.\n"
            f"Program operation is not guaranteed - you will likely encounter 429 errors\n"
            f"and temporary API bans.\n\n"
            f"Please consider reducing API_REQUESTS_PER_MINUTE to at most 1200",
        )
    elif 0 < API_REQUESTS_PER_MINUTE < 60:
        QMessageBox.information(
            None,  # type: ignore
            "Conservative API Rate",
            f"Low API request rate detected\n\nCurrent setting: {API_REQUESTS_PER_MINUTE} requests per minute\n\n"
            f"This is below the permitted rate of 60 requests per minute.\n"
            f"Consider setting API_REQUESTS_PER_MINUTE=60 for optimal performance",
        )
    elif API_REQUESTS_PER_MINUTE <= 0:
        QMessageBox.critical(
            None,  # type: ignore
            "No API Rate Limit",
            "API rate limiting is disabled\n\n"
            "You have disabled API rate limiting (API_REQUESTS_PER_MINUTE=0).\n\n"
            "This is extremely dangerous and will almost certainly result in\n"
            "your IP being temporarily banned from the osu! API.\n\n"
            "Please set API_REQUESTS_PER_MINUTE to at least 1 and at most 1200",
        )

class ValidationError(Exception):
    pass

class WorkerSignals(QObject):
    progress = Signal(int, int)
    log = Signal(str, bool)
    finished = Signal()
    error = Signal(str)
    result = Signal(object)

class Worker(QRunnable):
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

        try:
            fn_code = self.fn.__code__
            if "progress_callback" in fn_code.co_varnames:
                self.kwargs["progress_callback"] = partial(self.emit_progress)
            if "gui_log" in fn_code.co_varnames:
                self.kwargs["gui_log"] = partial(self.emit_log)
        except AttributeError:
            try:
                sig = inspect.signature(self.fn)
                if "progress_callback" in sig.parameters:
                    self.kwargs["progress_callback"] = partial(self.emit_progress)
                if "gui_log" in sig.parameters:
                    self.kwargs["gui_log"] = partial(self.emit_log)
            except Exception as e:
                logger.warning(f"Failed to inspect function {self.fn.__name__}: {e}")

    @Slot()
    def run(self):
        try:
            result = self.fn(*self.args, **self.kwargs)
            if result is not None:
                self.signals.result.emit(result)
        except ValidationError as ve:
            logger.info(f"A known validation error occurred: {ve}")
            self.signals.error.emit(str(ve))
        except Exception as e:
            logger.exception(f"Error in worker thread executing {self.fn.__name__}")
            self.signals.error.emit(str(e))
        finally:
            self.signals.finished.emit()

    def emit_progress(self, current, total):
        try:
            current = max(0, min(int(current), total))
            total = max(1, int(total))
            self.signals.progress.emit(current, total)
        except Exception as e:
            logger.warning(f"Error emitting progress: {e}")

    def emit_log(self, message, update_last=False):
        try:
            message = "None" if message is None else str(message)
            self.signals.log.emit(message, bool(update_last))
        except Exception as e:
            logger.warning(f"Error emitting log: {e}")

class IconHoverButton(QPushButton):
    def __init__(self, normal_icon=None, hover_icon=None, parent=None):
        super().__init__(parent)
        self.normal_icon = normal_icon or QIcon()
        self.hover_icon = hover_icon or QIcon()
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
        self.animation = QPropertyAnimation(self, QByteArray(b"value"))
        self.animation.setEasingCurve(QEasingCurve.Type.OutCubic)
        self.animation.setDuration(500)

    def setValue(self, value):
        self.animation.stop()
        self.animation.setStartValue(self.value())
        self.animation.setEndValue(value)
        self.animation.start()

class IconToggleButton(QPushButton):
    def __init__(self, icon_path_normal, icon_path_active, tooltip="", parent=None):
        super().__init__(parent)
        self.setCheckable(True)
        self.setToolTip(tooltip)
        self._icon_normal = QIcon(icon_path_normal)
        self._icon_active = QIcon(icon_path_active)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setObjectName("iconToggleButton")

        self.toggled.connect(self.update_style)
        self.update_style(self.isChecked())

    def update_style(self, checked):
        if checked:
            self.setIcon(self._icon_active)
            self.setProperty("class", "active")
        else:
            self.setIcon(self._icon_normal)
            self.setProperty("class", "")

        self.style().unpolish(self)
        self.style().polish(self)

    def sizeHint(self):
        return QSize(40, 40)

class UserProfileWidget(QFrame):
    custom_keys_requested = Signal()
    logout_requested = Signal()
    clear_cache_requested = Signal()
    config_changed = Signal()
    user_change_requested = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("userProfileWidget")

        self.main_window = parent
        self.api_client = None
        self.is_logged_in = False

        self.main_layout = QHBoxLayout(self)
        self.main_layout.setContentsMargins(15, 15, 15, 15)
        self.main_layout.setSpacing(20)

        self.avatar_label = QLabel()
        self.avatar_label.setObjectName("avatarLabel")
        self.avatar_label.setFixedSize(120, 120)

        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0, 0, 0, 0)
        self.content_layout.setSpacing(5)

        self.main_layout.addWidget(self.avatar_label)
        self.main_layout.addWidget(self.content_widget, 1)

        self._setup_logged_out_ui()

    def set_to_logged_out_state(self):
        self._setup_logged_out_ui()

    def set_controls_enabled(self, enabled: bool):
        if self.is_logged_in:
            if hasattr(self, 'change_user_button'):
                self.change_user_button.setEnabled(enabled)
                self.logout_button.setEnabled(enabled)
                self.unranked_toggle.setEnabled(enabled)
                self.missing_id_toggle.setEnabled(enabled)
                self.show_lost_toggle.setEnabled(enabled)
                self.scores_container.setEnabled(enabled)
                self.clear_cache_button.setEnabled(enabled)
                self.check_updates_button.setEnabled(enabled)
                self.nickname_stack.setEnabled(enabled)
        else:
            if hasattr(self, 'login_button'):
                self.login_button.setEnabled(enabled)
                self.custom_keys_button.setEnabled(enabled)

    def _clear_content_layout(self):
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            if widget := item.widget():
                widget.deleteLater()
            elif layout := item.layout():
                while layout.count():
                    sub_item = layout.takeAt(0)
                    if sub_widget := sub_item.widget():
                        sub_widget.deleteLater()
                layout.deleteLater()

    def update_state(self, user_data, api_client, config):
        self.api_client = api_client
        self._setup_logged_in_ui(user_data, config)
        self.update_stats_display(user_data)

    def _setup_logged_out_ui(self):
        self._clear_content_layout()
        self.set_default_avatar()

        # Create centered container for perfect center alignment
        center_widget = QWidget()
        center_layout = QVBoxLayout(center_widget)
        center_layout.setContentsMargins(0, 0, 0, 0)
        center_layout.setSpacing(0)

        # Title positioned higher
        title_label = QLabel("Connect your account")
        title_label.setObjectName("styledTitle")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # OAuth button with backdrop container
        button_container = QFrame()
        button_container.setObjectName("buttonBackdrop")
        button_layout = QVBoxLayout(button_container)
        button_layout.setContentsMargins(8, 8, 8, 8)
        button_layout.setSpacing(0)

        self.login_button = QPushButton("Login with osu!")
        self.login_button.setObjectName("frontendStyledButton")
        self.login_button.setToolTip("Secure login using your osu! account (Recommended)")
        if self.main_window and hasattr(self.main_window, '_on_oauth_login_clicked'):
            self.login_button.clicked.connect(self.main_window._on_oauth_login_clicked)

        button_layout.addWidget(self.login_button)

        # Simple divider
        divider_label = QLabel("or")
        divider_label.setObjectName("styledDivider")
        divider_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # Smaller API button positioned higher
        self.custom_keys_button = QPushButton("⚙️ Use Custom API Keys")
        self.custom_keys_button.setObjectName("compactApiButton")
        self.custom_keys_button.setToolTip("For advanced users with their own osu! API credentials")
        self.custom_keys_button.clicked.connect(self.custom_keys_requested.emit)

        # Build center container
        center_layout.addWidget(title_label)
        center_layout.addSpacing(5)   # Move button even higher
        center_layout.addWidget(button_container, 0, Qt.AlignmentFlag.AlignHCenter)
        center_layout.addSpacing(6)   # More space from larger button
        center_layout.addWidget(divider_label)
        center_layout.addSpacing(2)   # Less space to smaller button
        center_layout.addWidget(self.custom_keys_button, 0, Qt.AlignmentFlag.AlignHCenter)

        # Perfect center alignment in main layout
        self.content_layout.addStretch(1)
        self.content_layout.addWidget(center_widget, 0, Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter)
        self.content_layout.addStretch(1)
        self.is_logged_in = False

    def _setup_logged_in_ui(self, user_data, config):
        self._clear_content_layout()

        top_layout = QHBoxLayout()
        top_layout.setSpacing(10)

        self.nickname_stack = QStackedWidget()
        self.nickname_label = QLabel()
        self.nickname_label.setObjectName("nicknameLabel")
        self.nickname_input = QLineEdit()
        self.nickname_input.setObjectName("nicknameInput")
        self.nickname_input.editingFinished.connect(self._confirm_user_change)
        self.nickname_stack.addWidget(self.nickname_label)
        self.nickname_stack.addWidget(self.nickname_input)
        self._update_nickname_display(user_data["username"])

        self.change_user_button = IconHoverButton(
            QIcon(os.path.join(ICON_PATH, "edit_user.png")),
            QIcon(os.path.join(ICON_PATH, "edit_user_hover.png")),
        )
        self.change_user_button.setToolTip("Change user")
        self.change_user_button.setFixedSize(30, 30)
        self.change_user_button.clicked.connect(self._toggle_edit_mode)

        self.logout_button = IconHoverButton(
            QIcon(os.path.join(ICON_PATH, "logout.png")),
            QIcon(os.path.join(ICON_PATH, "logout_hover.png")),
        )
        self.logout_button.setToolTip("Log out")
        self.logout_button.setFixedSize(30, 30)
        self.logout_button.clicked.connect(self.logout_requested.emit)

        if self.main_window and hasattr(self.main_window, 'auth_manager'):
            current_session = self.main_window.auth_manager.get_current_session()
            if current_session.auth_mode == AuthMode.OAUTH:
                self.change_user_button.setVisible(False)

        top_layout.addWidget(self.nickname_stack, 1)
        top_layout.addWidget(self.change_user_button)
        top_layout.addWidget(self.logout_button)
        self.content_layout.addLayout(top_layout)

        self.stats_widget = QLabel("Fetching initial stats...")
        self.stats_widget.setObjectName("statsWidget")
        self.stats_widget.setWordWrap(True)
        self.content_layout.addWidget(self.stats_widget)
        self.content_layout.addStretch()

        toggle_layout = QHBoxLayout()
        toggle_layout.setSpacing(10)
        self.unranked_toggle = IconToggleButton(
            get_standard_dir("assets/images/icons/unranked_off.png"),
            get_standard_dir("assets/images/icons/unranked_on.png"),
            "Include unranked and loved beatmaps",
        )
        self.unranked_toggle.setChecked(config.get("include_unranked", False))
        self.unranked_toggle.toggled.connect(lambda: self.config_changed.emit())

        self.missing_id_toggle = IconToggleButton(
            get_standard_dir("assets/images/icons/missing_id_off.png"),
            get_standard_dir("assets/images/icons/missing_id_on.png"),
            "Check missing beatmap IDs (may take a long time)",
        )
        self.missing_id_toggle.setChecked(config.get("check_missing_ids", False))
        self.missing_id_toggle.toggled.connect(lambda: self.config_changed.emit())

        self.show_lost_toggle = IconToggleButton(
            get_standard_dir("assets/images/icons/show_lost_off.png"),
            get_standard_dir("assets/images/icons/show_lost_on.png"),
            "Ensure at least one lost score is visible in the top plays image",
        )
        self.show_lost_toggle.setChecked(config.get("show_lost", False))
        self.show_lost_toggle.toggled.connect(lambda: self.config_changed.emit())

        toggle_layout.addWidget(self.unranked_toggle)
        toggle_layout.addWidget(self.missing_id_toggle)
        toggle_layout.addWidget(self.show_lost_toggle)

        self.content_layout.addLayout(toggle_layout)
        self.content_layout.addStretch()

        bottom_controls_layout = QHBoxLayout()
        bottom_controls_layout.setContentsMargins(0, 8, 0, 0)
        bottom_controls_layout.setSpacing(10)

        scores_label = QLabel("Scores to show:")
        scores_label.setObjectName("scoresLabel")
        self.scores_container = QFrame()
        self.scores_container.setObjectName("scoresContainer")
        scores_container_layout = QHBoxLayout(self.scores_container)
        scores_container_layout.setContentsMargins(8, 2, 4, 2)
        scores_container_layout.setSpacing(4)

        self.scores_count_stack = QStackedWidget()
        self.scores_count_stack.setFixedSize(40, 28)
        # Ensure we have a valid scores_count value
        scores_count_value = config.get("scores_count", 10) if config else 10
        if not scores_count_value or scores_count_value == "":
            scores_count_value = 10
        self.scores_count_display = QLabel(str(scores_count_value))
        self.scores_count_display.setObjectName("scoresCountDisplay")
        self.scores_count_display.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.scores_count_input = QLineEdit()
        self.scores_count_input.setObjectName("scoresCountInput")
        self.scores_count_input.setValidator(QtGui.QIntValidator(1, 999, self))
        self.scores_count_input.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.scores_count_input.editingFinished.connect(self._confirm_scores_change)
        self.scores_count_stack.addWidget(self.scores_count_display)
        self.scores_count_stack.addWidget(self.scores_count_input)

        self.edit_scores_button = IconHoverButton(
            QIcon(os.path.join(ICON_PATH, "edit.png")),
            QIcon(os.path.join(ICON_PATH, "edit_hover.png")),
        )
        self.edit_scores_button.setObjectName("editScoresButton")
        self.edit_scores_button.setFixedSize(28, 28)
        self.edit_scores_button.setToolTip("Edit number of scores to display")
        self.edit_scores_button.clicked.connect(self._toggle_scores_edit)
        scores_container_layout.addWidget(self.scores_count_stack)
        scores_container_layout.addWidget(self.edit_scores_button)
        bottom_controls_layout.addWidget(scores_label)
        bottom_controls_layout.addWidget(self.scores_container)
        bottom_controls_layout.addStretch(1)

        self.clear_cache_button = IconHoverButton(
            QIcon(os.path.join(ICON_PATH, "clear_cache.png")),
            QIcon(os.path.join(ICON_PATH, "clear_cache_hover.png")),
        )
        self.clear_cache_button.setToolTip("Clear cache")
        self.clear_cache_button.setFixedSize(35, 35)
        self.clear_cache_button.setObjectName("iconActionButton")
        self.clear_cache_button.clicked.connect(self.clear_cache_requested.emit)

        self.check_updates_button = IconHoverButton(
            QIcon(os.path.join(ICON_PATH, "check_updates.png")),
            QIcon(os.path.join(ICON_PATH, "check_updates_hover.png")),
        )
        self.check_updates_button.setToolTip("Check for updates")
        self.check_updates_button.setFixedSize(35, 35)
        self.check_updates_button.setObjectName("iconActionButton")

        bottom_controls_layout.addWidget(self.clear_cache_button)
        bottom_controls_layout.addWidget(self.check_updates_button)

        self.content_layout.addLayout(bottom_controls_layout)
        self.is_logged_in = True

    def _toggle_edit_mode(self):
        if self.nickname_stack.currentIndex() == 0:
            self.nickname_input.setText(self.nickname_label.text())
            self.nickname_stack.setCurrentIndex(1)
            self.nickname_input.setFocus()
            self.nickname_input.selectAll()

    def _confirm_user_change(self):
        new_username = self.nickname_input.text().strip()
        current_username = self.nickname_label.text()
        self.nickname_stack.setCurrentIndex(0)

        if not new_username or new_username.lower() == current_username.lower():
            return

        reply = QMessageBox.question(
            self,
            "Confirm User Change",
            f"Are you sure you want to change user to '{new_username}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            self.user_change_requested.emit(new_username)

    def _update_nickname_display(self, username):
        self.nickname_label.setToolTip(username)
        font_metrics = QtGui.QFontMetrics(self.nickname_label.font())
        elided_text = font_metrics.elidedText(
            username, Qt.TextElideMode.ElideRight, 350
        )
        self.nickname_label.setText(elided_text)
        self.nickname_input.setText(username)

    def _toggle_scores_edit(self):
        if self.scores_count_stack.currentIndex() == 0:
            self.scores_count_input.setText(self.scores_count_display.text())
            self.scores_count_stack.setCurrentIndex(1)
            self.scores_count_input.setFocus()
            self.scores_count_input.selectAll()
        else:
            self._confirm_scores_change()

    def _confirm_scores_change(self):
        new_value = self.scores_count_input.text()
        if new_value.isdigit() and 1 <= int(new_value) <= 999:
            self.scores_count_display.setText(new_value)
            self.config_changed.emit()
        else:
            self.scores_count_input.setText(self.scores_count_display.text())
        self.scores_count_stack.setCurrentIndex(0)

    def update_stats_display(self, user_data, scan_data=None):
        stats_text = ""
        stats = user_data.get("statistics", {})
        pp = float(stats.get("pp", 0))
        acc = float(stats.get("hit_accuracy", 0))
        rank = stats.get("global_rank", 0)
        rank_str = f"#{int(rank):,}" if rank else "#N/A"

        if scan_data:
            try:
                potential_pp = float(scan_data.get("potential_pp", pp))
                potential_acc = float(scan_data.get("potential_acc", acc))

                pp_diff = potential_pp - pp
                acc_diff = potential_acc - acc

                pp_color_tuple = get_delta_color(pp_diff)
                acc_color_tuple = get_delta_color(acc_diff)

                pp_color_hex = '#%02x%02x%02x' % pp_color_tuple
                acc_color_hex = '#%02x%02x%02x' % acc_color_tuple

                pp_str = (
                    f"{round(pp):,} → <b style='color:{pp_color_hex};'>{round(potential_pp):,}</b>"
                )
                acc_str = (
                    f"{acc:.2f}% → <b style='color:{acc_color_hex};'>{potential_acc:.2f}%</b>"
                )
                stats_text = f"{pp_str} <span style='color: {SEPARATOR_COLOR};'>|</span> {acc_str} <span style='color: {SEPARATOR_COLOR};'>|</span> {rank_str}"
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not parse scan_data for stats display: {e}")
                scan_data = None

        if not scan_data:
            pp_str = f"{round(pp):,}"
            acc_str = f"{acc:.2f}%"
            stats_text = (
                f"<span style='color: {TEXT_SECONDARY};'>PP:</span> <b style='color: {TEXT_PRIMARY};'>{pp_str}</b>"
                f" <span style='color: {TEXT_SECONDARY};'>| Acc:</span> <b style='color: {TEXT_PRIMARY};'>{acc_str}</b>"
                f" <span style='color: {TEXT_SECONDARY};'>| Rank:</span> <b style='color: {TEXT_PRIMARY};'>{rank_str}</b>"
            )

        self.stats_widget.setText(stats_text.strip())

    def set_default_avatar(self):
        default_avatar_path = get_standard_dir(
            "assets/images/default_avatar/default_avatar.png"
        )
        if os.path.exists(default_avatar_path):
            self.set_avatar(default_avatar_path)

    def set_avatar(self, image_path):
        pixmap = QPixmap(image_path)
        if pixmap.isNull():
            self.set_default_avatar()
            return

        size = self.avatar_label.size()
        rounded_pixmap = QPixmap(size)
        rounded_pixmap.fill(Qt.GlobalColor.transparent)

        painter = QPainter(rounded_pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        path = QtGui.QPainterPath()
        path.addRoundedRect(QRect(0, 0, size.width(), size.height()), 20, 20)
        painter.setClipPath(path)

        scaled_pixmap = pixmap.scaled(
            size,
            Qt.AspectRatioMode.KeepAspectRatioByExpanding,
            Qt.TransformationMode.SmoothTransformation,
        )
        painter.drawPixmap(0, 0, scaled_pixmap)
        painter.end()
        self.avatar_label.setPixmap(rounded_pixmap)

class ApiDialog(QDialog):
    def __init__(
            self,
            parent=None,
            client_id="",
            client_secret="",
            username="",
    ):
        super().__init__(parent)
        self.is_secret_visible = False
        self.setWindowTitle("API Keys & User Configuration")
        self.setFixedSize(440, 360)
        self.setObjectName("apiDialog")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(10)

        username_label_layout = QHBoxLayout()
        username_label = QLabel("Username or ID:")
        self.username_error_label = QLabel()
        self.username_error_label.setObjectName("errorLabel")
        self.username_error_label.setVisible(False)
        username_label_layout.addWidget(username_label)
        username_label_layout.addStretch()
        username_label_layout.addWidget(self.username_error_label)

        self.username_input = QLineEdit(username)
        self.username_input.setObjectName("usernameInput")
        self.username_input.setMinimumHeight(35)
        self.username_input.textChanged.connect(
            lambda: self.clear_error_state(
                self.username_input, self.username_error_label
            )
        )
        layout.addLayout(username_label_layout)
        layout.addWidget(self.username_input)

        id_label_layout = QHBoxLayout()
        id_label = QLabel("Client ID:")
        self.id_error_label = QLabel()
        self.id_error_label.setObjectName("errorLabel")
        self.id_error_label.setVisible(False)
        id_label_layout.addWidget(id_label)
        id_label_layout.addStretch()
        id_label_layout.addWidget(self.id_error_label)

        self.id_input = QLineEdit(client_id)
        self.id_input.setObjectName("idInput")
        self.id_input.setMinimumHeight(35)
        self.id_input.textChanged.connect(
            lambda: self.clear_error_state(self.id_input, self.id_error_label)
        )
        layout.addLayout(id_label_layout)
        layout.addWidget(self.id_input)

        secret_label_layout = QHBoxLayout()
        secret_label = QLabel("Client Secret:")
        self.secret_error_label = QLabel()
        self.secret_error_label.setObjectName("errorLabel")
        self.secret_error_label.setVisible(False)
        secret_label_layout.addWidget(secret_label)
        secret_label_layout.addStretch()
        secret_label_layout.addWidget(self.secret_error_label)

        self.secret_input = QLineEdit(client_secret)
        self.secret_input.setObjectName("secretInput")
        self.secret_input.setMinimumHeight(35)
        self.secret_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.secret_input.textChanged.connect(
            lambda: self.clear_error_state(
                self.secret_container, self.secret_error_label
            )
        )
        self.secret_container = QFrame()
        self.secret_container.setObjectName("secretContainer")
        secret_container_layout = QHBoxLayout(self.secret_container)
        secret_container_layout.setContentsMargins(10, 0, 10, 0)
        secret_container_layout.setSpacing(0)
        self.show_secret_btn = IconHoverButton(
            QIcon(os.path.join(ICON_PATH, "eye_closed.png")),
            QIcon(os.path.join(ICON_PATH, "eye_closed_hover.png")),
        )
        self.show_secret_btn.setObjectName("showSecretBtn")
        self.show_secret_btn.setFixedSize(30, 30)
        self.show_secret_btn.clicked.connect(self.toggle_secret_visibility)
        secret_container_layout.addWidget(self.secret_input, 1)
        secret_container_layout.addWidget(self.show_secret_btn, 0)

        layout.addLayout(secret_label_layout)
        layout.addWidget(self.secret_container)

        self.help_label = QLabel(
            f'<a href="https://osu.ppy.sh/home/account/edit#oauth" style="color:{LINK_COLOR};">How to get API keys?</a>'
        )
        self.help_label.setObjectName("helpLabel")
        self.help_label.setOpenExternalLinks(True)
        layout.addWidget(self.help_label)
        layout.addStretch(1)

        button_layout = QHBoxLayout()
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.reject)
        self.save_btn = QPushButton("Save")
        self.save_btn.clicked.connect(self.validate_and_accept)
        button_layout.addWidget(self.cancel_btn)
        button_layout.addWidget(self.save_btn)
        layout.addLayout(button_layout)

    def validate_and_accept(self):
        self.clear_error_state(self.username_input, self.username_error_label)
        self.clear_error_state(self.id_input, self.id_error_label)
        self.clear_error_state(self.secret_container, self.secret_error_label)

        is_valid = True
        if not self.username_input.text().strip():
            self.show_error(self.username_input, self.username_error_label, "required")
            is_valid = False
        if not self.id_input.text().strip():
            self.show_error(self.id_input, self.id_error_label, "required")
            is_valid = False
        if not self.secret_input.text().strip():
            self.show_error(self.secret_container, self.secret_error_label, "required")
            is_valid = False

        if is_valid:
            super().accept()

    def show_error(self, line_edit_widget, label, text):
        label.setText(f'<span style="color: {ERROR_COLOR};">{text}</span>')
        label.setVisible(True)
        line_edit_widget.setProperty("state", "error")
        self.style().unpolish(line_edit_widget)
        self.style().polish(line_edit_widget)

    def clear_error_state(self, line_edit_widget, label=None):
        if label:
            label.setVisible(False)

        widget_to_style = (
            self.secret_container
            if line_edit_widget is self.secret_input
            else line_edit_widget
        )
        widget_to_style.setProperty("state", "")
        self.style().unpolish(widget_to_style)
        self.style().polish(widget_to_style)

    def toggle_secret_visibility(self):
        self.is_secret_visible = not getattr(self, "is_secret_visible", False)

        if self.is_secret_visible:
            echo_mode = QLineEdit.EchoMode.Normal
            icon_name = "eye_open"
        else:
            echo_mode = QLineEdit.EchoMode.Password
            icon_name = "eye_closed"

        self.secret_input.setEchoMode(echo_mode)
        button = self.show_secret_btn
        button.normal_icon = QIcon(os.path.join(ICON_PATH, f"{icon_name}.png"))
        button.hover_icon = QIcon(os.path.join(ICON_PATH, f"{icon_name}_hover.png"))

        if button.underMouse():
            button.setIcon(button.hover_icon)
        else:
            button.setIcon(button.normal_icon)

    # noinspection PyMethodMayBeStatic
    def show_context_menu(self, widget, position):
        menu = create_standard_edit_menu(widget)
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

        col_name = self._data.columns[index.column()]
        value = self._data.iloc[index.row(), index.column()]

        if role == Qt.ItemDataRole.DisplayRole:
            if col_name == "Rank":
                if value == "XH":
                    return "SSH"
                if value == "X":
                    return "SS"
            if col_name in ["Score ID", "Score"]:
                if pd.notna(value) and value != "LOST":
                    try:
                        return str(int(float(value)))
                    except (ValueError, TypeError):
                        return str(value)
                return str(value)
            if isinstance(value, (float, int)):
                if col_name in ["100", "50", "Misses"]:
                    return str(int(value)) if pd.notna(value) else ""
                if col_name == "Accuracy":
                    return f"{value:.2f}"
            return str(value)

        if role == Qt.ItemDataRole.BackgroundRole:
            return QCOLOR_PRIMARY_BG() if index.row() % 2 == 0 else QCOLOR_SECONDARY_BG()

        if role == Qt.ItemDataRole.TextAlignmentRole:
            if isinstance(value, (int, float)):
                return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            return Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter

        if role == Qt.ItemDataRole.ForegroundRole:
            score_id_col = "Score ID" if "Score ID" in self._data.columns else None
            if score_id_col:
                score_id_loc = self._data.columns.get_loc(score_id_col)
                score_id_value = str(self._data.iloc[index.row(), score_id_loc])
                if score_id_value == "LOST" and col_name in ["PP", score_id_col]:
                    return QCOLOR_ACCENT()
            return QCOLOR_TEXT_PRIMARY()

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

    def sort(self, column: int, order: Qt.SortOrder = Qt.SortOrder.AscendingOrder) -> None:
        try:
            if column >= len(self._data.columns):
                return

            col_name = self._data.columns[column]
            ascending = order == Qt.SortOrder.AscendingOrder
            self.layoutAboutToBeChanged.emit()

            if col_name == "Mods":

                def mod_sort_key(mod_str):
                    if not mod_str or pd.isna(mod_str):
                        return 0, ""
                    mods = mod_str.split(", ")
                    if "NC" in mods:
                        mods = [m for m in mods if m != "NC"] + ["DT+"]
                    mod_count = 0 if len(mods) == 1 and mods[0] == "NM" else len(mods)
                    return mod_count, ", ".join(sorted(mods))

                temp_df = self._data.copy()
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

                def score_id_sort_key(id_str):
                    if str(id_str) == "LOST":
                        return 0 if not ascending else float("inf")
                    try:
                        return int(float(id_str))
                    except (ValueError, TypeError):
                        return id_str

                temp_df = self._data.copy()
                temp_df["id_sort_key"] = temp_df[col_name].apply(score_id_sort_key)
                self._data = temp_df.sort_values(
                    "id_sort_key", ascending=ascending
                ).drop("id_sort_key", axis=1)

            elif col_name == "Date":

                def parse_date_safe(date_str):
                    if pd.isna(date_str):
                        return pd.NaT
                    date_str = str(date_str).strip().replace("...", "").strip()
                    for fmt in [
                        "%d-%m-%Y %H:%M:%S",
                        "%d-%m-%Y",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d",
                    ]:
                        try:
                            return pd.to_datetime(date_str, format=fmt)
                        except (ValueError, TypeError):
                            continue
                    return pd.to_datetime(date_str, errors="coerce")

                temp_df = self._data.copy()
                temp_df["date_sort_key"] = temp_df[col_name].apply(parse_date_safe)
                self._data = temp_df.sort_values(
                    "date_sort_key", ascending=ascending, na_position="last"
                ).drop("date_sort_key", axis=1)

            else:
                self._data = self._data.sort_values(
                    by=col_name, ascending=ascending, na_position="last"
                )

            self.layoutChanged.emit()
        except (TypeError, ValueError, KeyError) as e:
            logger.error(f"Error sorting table: {e}")

    def get_dataframe(self):
        return self._data

# noinspection PyTypedDict
class ResultsWindow(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        screen = QApplication.primaryScreen().availableGeometry()
        self.resize(int(screen.width() * 0.8), int(screen.height() * 0.8))
        self.setWindowTitle("Full Scan Results")
        self.setObjectName("resultsWindow")
        logger.debug("Initializing ResultsWindow")

        self.stats_data = {"lost_scores": {}, "parsed_top": {}, "top_with_lost": {}}
        self.search_results = []
        self.current_result_index = -1

        self.setWindowFlags(
            Qt.WindowType.Dialog
            | Qt.WindowType.WindowSystemMenuHint
            | Qt.WindowType.WindowMinMaxButtonsHint
            | Qt.WindowType.WindowCloseButtonHint
        )

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)

        title_layout = QHBoxLayout()
        title_layout.setContentsMargins(0, 0, 0, 5)
        self.scan_time_label = QLabel("Last scan: Unknown")
        title_layout.addWidget(self.scan_time_label, 1)

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
        self.prev_result_button.setFixedSize(30, 30)
        self.prev_result_button.clicked.connect(self.go_to_previous_result)
        self.prev_result_button.setVisible(False)
        search_layout.addWidget(self.prev_result_button)

        self.next_result_button = QPushButton("▼", self.search_container)
        self.next_result_button.setObjectName("nextResultButton")
        self.next_result_button.setFixedSize(30, 30)
        self.next_result_button.clicked.connect(self.go_to_next_result)
        self.next_result_button.setVisible(False)
        search_layout.addWidget(self.next_result_button)

        self.search_input = QLineEdit(self.search_container)
        self.search_input.setObjectName("searchInput")
        self.search_input.setPlaceholderText("Search in table...")
        self.search_input.setMinimumHeight(30)
        self.search_input.returnPressed.connect(self.perform_search)
        self.search_input.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.search_input.customContextMenuRequested.connect(
            lambda pos: self.show_context_menu(self.search_input, pos)
        )
        search_layout.addWidget(self.search_input)

        self.search_button = QPushButton("Find", self.search_container)
        self.search_button.setObjectName("searchButton")
        self.search_button.setMinimumHeight(30)
        self.search_button.setMinimumWidth(70)
        self.search_button.clicked.connect(self.perform_search)
        search_layout.addWidget(self.search_button)

        title_layout.addWidget(self.search_container, 0)
        main_layout.addLayout(title_layout)

        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)

        self.lost_scores_tab, self.lost_scores_view = self._create_table_tab()
        self.parsed_top_tab, self.parsed_top_view = self._create_table_tab()
        self.top_with_lost_tab, self.top_with_lost_view = self._create_table_tab()

        self.tab_widget.addTab(self.lost_scores_tab, "Lost Scores")
        self.tab_widget.addTab(self.parsed_top_tab, "Online Top")
        self.tab_widget.addTab(self.top_with_lost_tab, "Potential Top")

        self.bottom_layout = QHBoxLayout()
        self.bottom_layout.setContentsMargins(0, 5, 0, 0)
        self.stats_panel = QFrame()
        self.stats_panel.setObjectName("StatsPanel")
        self.stats_panel.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred
        )
        self.stats_panel_layout = QHBoxLayout(self.stats_panel)
        self.stats_panel_layout.setContentsMargins(10, 5, 10, 5)
        self.stats_panel_layout.setSpacing(20)
        self.bottom_layout.addWidget(self.stats_panel, 1)

        self.close_button = QPushButton("Close")
        self.close_button.setObjectName("closeButton")
        self.close_button.setProperty("class", "min-close-button")
        self.close_button.clicked.connect(self.close)
        self.bottom_layout.addWidget(self.close_button, 0)
        main_layout.addLayout(self.bottom_layout)

        self.search_button.setAutoDefault(True)
        self.search_button.setDefault(True)
        self.close_button.setAutoDefault(False)
        self.close_button.setDefault(False)

        shortcut_search = QShortcut(QKeySequence("Ctrl+F"), self)
        shortcut_search.activated.connect(self.focus_search)

        self.lost_scores_view.customContextMenuRequested.connect(
            lambda pos: self.show_table_context_menu(self.lost_scores_view, pos)
        )
        self.parsed_top_view.customContextMenuRequested.connect(
            lambda pos: self.show_table_context_menu(self.parsed_top_view, pos)
        )
        self.top_with_lost_view.customContextMenuRequested.connect(
            lambda pos: self.show_table_context_menu(self.top_with_lost_view, pos)
        )

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

        QtCore.QTimer.singleShot(100, self.load_data)
        self.tab_widget.currentChanged.connect(self.update_stats_panel)
        self.update_stats_panel(self.tab_widget.currentIndex())
        self.focus_initial_table()

    def _create_table_tab(self):
        tab_widget = QWidget()
        layout = QVBoxLayout(tab_widget)
        layout.setContentsMargins(0, 0, 0, 0)

        table_view = QTableView()
        self.setup_table_view(table_view)

        layout.addWidget(table_view, 1)

        return tab_widget, table_view

    @staticmethod
    def setup_table_view(table_view):
        table_view.setSortingEnabled(True)
        table_view.horizontalHeader().setStretchLastSection(False)
        table_view.verticalHeader().setDefaultSectionSize(30)
        table_view.verticalHeader().setFixedWidth(40)
        table_view.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )
        table_view.setSelectionMode(QTableView.SelectionMode.ExtendedSelection)
        table_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)

    def focus_initial_table(self):
        current_view = self.tab_widget.currentWidget().findChild(QTableView)
        if current_view:
            current_view.setFocus()

    def load_data(self):
        try:
            self.update_scan_time()

            lost_scores_path = get_standard_dir("csv/lost_scores.csv")
            self.load_table_data(self.lost_scores_view, lost_scores_path, None)

            parsed_top_path = get_standard_dir("csv/parsed_top.csv")
            self.load_table_data(self.parsed_top_view, parsed_top_path, None)

            top_with_lost_path = get_standard_dir("csv/top_with_lost.csv")
            self.load_table_data(
                self.top_with_lost_view, top_with_lost_path, None, hide_status_col=True
            )

            self._load_and_process_summary_stats()

            self.update_stats_panel(self.tab_widget.currentIndex())
            
        except Exception as e:
            logger.error(f"Error loading data in ResultsWindow: {e}")
            error_df = pd.DataFrame({"Error": [f"Failed to load results data: {e}"]})
            model = PandasTableModel(error_df)
            self.lost_scores_view.setModel(model)
            self.parsed_top_view.setModel(model)
            self.top_with_lost_view.setModel(model)

    def load_table_data(
            self, table_view, file_path, stats_calculator, hide_status_col=False
    ):
        try:
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                data_df = pd.read_csv(file_path)
                model = PandasTableModel(data_df)
                table_view.setModel(model)
                self.setup_column_widths(table_view)
                if hide_status_col:
                    try:
                        status_col_index = data_df.columns.get_loc("Status")
                        table_view.hideColumn(status_col_index)
                    except KeyError:
                        pass
                if stats_calculator:
                    stats_calculator(data_df)
            else:
                empty_df = pd.DataFrame({"Status": ["No data found. Run a scan first"]})
                model = PandasTableModel(empty_df)
                table_view.setModel(model)
        except Exception as e:
            logger.error(f"Error processing {os.path.basename(file_path)}: {e}")
            error_df = pd.DataFrame({"Error": [f"Could not load data: {e}"]})
            model = PandasTableModel(error_df)
            table_view.setModel(model)

    def update_scan_time(self):
        try:
            csv_files = [
                get_standard_dir("csv/lost_scores.csv"),
                get_standard_dir("csv/parsed_top.csv"),
                get_standard_dir("csv/top_with_lost.csv"),
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
            logger.error(f"Error updating scan time: {e}")
            self.scan_time_label.setText("Last scan: Error checking time")

    @staticmethod
    def setup_column_widths(table_view):
        try:
            header = table_view.horizontalHeader()
            model = table_view.model()
            if not model or model.columnCount() == 0:
                return

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
                    header.resizeSection(col_idx, default_widths[col_name])
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
                    header.setSectionResizeMode(col_idx, QHeaderView.ResizeMode.Fixed)

            try:
                beatmap_col_idx = list(model.get_dataframe().columns).index("Beatmap")
                header.setSectionResizeMode(
                    beatmap_col_idx, QHeaderView.ResizeMode.Stretch
                )
            except ValueError:
                header.setStretchLastSection(True)
        except Exception as e:
            logger.error(f"Error setting column widths: {e}")

    def _load_and_process_summary_stats(self):
        summary_data = load_summary_stats()
        if not summary_data:
            logger.warning("Could not load summary stats for ResultsWindow")
            self.stats_data = {"lost_scores": {}, "parsed_top": {}, "top_with_lost": {}}
            return

        try:
            self.stats_data["lost_scores"] = {
                "total": int(summary_data.get("post_filter_count", 0)),
                "avg_pp_lost_diff": float(summary_data.get("avg_pp_lost_diff", 0.0)),
                "avg_pp_lost_diff_count": int(
                    summary_data.get("avg_pp_lost_diff_count", 0)
                ),
            }
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing lost_scores stats: {e}")
            self.stats_data["lost_scores"] = {}

        try:
            self.stats_data["parsed_top"] = {
                "Overall PP": f"{float(summary_data.get('current_pp', 0)):.2f}",
                "Overall Accuracy": f"{float(summary_data.get('current_acc', 0)):.2f}%",
            }
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing parsed_top stats: {e}")
            self.stats_data["parsed_top"] = {}

        try:
            self.stats_data["top_with_lost"] = {
                "current_pp": float(summary_data.get("current_pp", 0.0)),
                "potential_pp": float(summary_data.get("potential_pp", 0.0)),
                "delta_pp": float(summary_data.get("delta_pp", 0.0)),
                "current_acc": float(summary_data.get("current_acc", 0.0)),
                "potential_acc": float(summary_data.get("potential_acc", 0.0)),
                "delta_acc": float(summary_data.get("delta_acc", 0.0)),
            }
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing top_with_lost stats: {e}")
            self.stats_data["top_with_lost"] = {}

    def focus_search(self):
        self.search_input.setFocus()
        self.search_input.selectAll()

    def perform_search(self):
        search_text = self.search_input.text().strip().lower()
        current_table = self.tab_widget.currentWidget().findChild(QTableView)

        if not search_text or not current_table:
            self.search_results.clear()
            self.current_result_index = -1
            self.update_search_ui()
            return

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
                    self.search_results.append(idx)

        if self.search_results:
            self.current_result_index = 0
            self.highlight_current_result()
        else:
            QMessageBox.information(
                self, "Search Results", f"No matches found for '{search_text}'"
            )

        self.update_search_ui()

    def update_stats_panel(self, tab_index):
        self.clear_stats_panel()
        layout = self.stats_panel_layout

        try:
            if tab_index == 0:
                stats = self.stats_data.get("lost_scores", {})
                total_scores = stats.get("total", 0)

                if total_scores > 0:
                    layout.addWidget(QLabel(f"Total Found: {total_scores}"))
                    avg_diff = stats.get("avg_pp_lost_diff", 0.0)
                    diff_count = stats.get("avg_pp_lost_diff_count", 0)
                    if avg_diff > 0 and diff_count > 0:
                        diff_text = f"Average PP lost: {avg_diff:.2f}"
                        layout.addWidget(QLabel(diff_text))
                else:
                    layout.addWidget(QLabel("No lost scores found"))

            elif tab_index == 1:
                stats = self.stats_data.get("parsed_top", {})
                if not stats:
                    layout.addWidget(QLabel("No statistics available"))
                else:
                    layout.addWidget(
                        QLabel(f"Overall PP: {stats.get('Overall PP', 'N/A')}")
                    )
                    layout.addWidget(
                        QLabel(
                            f"Overall Accuracy: {stats.get('Overall Accuracy', 'N/A')}"
                        )
                    )

            elif tab_index == 2:
                stats = self.stats_data.get("top_with_lost", {})
                if not stats:
                    layout.addWidget(QLabel("No statistics available"))
                else:
                    pp_diff = stats.get("delta_pp", 0.0)
                    acc_diff = stats.get("delta_acc", 0.0)

                    pp_color_tuple = get_delta_color(pp_diff)
                    acc_color_tuple = get_delta_color(acc_diff)

                    pp_color_hex = '#%02x%02x%02x' % pp_color_tuple
                    acc_color_hex = '#%02x%02x%02x' % acc_color_tuple

                    layout.addWidget(QLabel(f"Current PP: {stats.get('current_pp', 0.0):.2f}"))
                    layout.addWidget(QLabel(f"Potential PP: {stats.get('potential_pp', 0.0):.2f}"))
                    layout.addWidget(
                        QLabel(
                            f"<span style='color:{pp_color_hex}'>Δ PP: <b>{pp_diff:+.2f}</b></span>"
                        )
                    )
                    layout.addSpacing(20)
                    layout.addWidget(QLabel(f"Current Acc: {stats.get('current_acc', 0.0):.2f}%"))
                    layout.addWidget(QLabel(f"Potential Acc: {stats.get('potential_acc', 0.0):.2f}%"))
                    layout.addWidget(
                        QLabel(
                            f"<span style='color:{acc_color_hex}'>Δ Acc: <b>{acc_diff:+.2f}%</b></span>"
                        )
                    )

            layout.addStretch()
        except Exception as e:
            logger.error(f"Error updating stats panel: {e}")
            self.clear_stats_panel()
            layout.addWidget(QLabel(f"Error updating stats: {e}"))
            layout.addStretch()

    def clear_stats_panel(self):
        layout = self.stats_panel_layout
        if not layout:
            return
        while layout.count():
            item = layout.takeAt(0)
            if widget := item.widget():
                widget.deleteLater()

    def update_search_ui(self):
        count = len(self.search_results)
        self.prev_result_button.setVisible(count > 1)
        self.next_result_button.setVisible(count > 1)
        if count > 0:
            self.search_count_label.setText(f"{self.current_result_index + 1}/{count}")
        else:
            self.search_count_label.setText("")

    def highlight_current_result(self):
        if not self.search_results:
            return

        current_table = self.tab_widget.currentWidget().findChild(QTableView)
        if not current_table:
            return

        index_to_select = self.search_results[self.current_result_index]
        current_table.clearSelection()
        current_table.selectionModel().select(
            index_to_select,
            QtCore.QItemSelectionModel.SelectionFlag.Select
            | QtCore.QItemSelectionModel.SelectionFlag.Rows,
        )
        current_table.scrollTo(index_to_select, QTableView.ScrollHint.PositionAtCenter)
        self.update_search_ui()

    def go_to_next_result(self):
        if not self.search_results:
            return
        self.current_result_index = (self.current_result_index + 1) % len(
            self.search_results
        )
        self.highlight_current_result()

    def go_to_previous_result(self):
        if not self.search_results:
            return
        self.current_result_index = (
                                            self.current_result_index - 1 + len(self.search_results)
                                    ) % len(self.search_results)
        self.highlight_current_result()

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

    @staticmethod
    def copy_selected_cells(table_view):
        selected = table_view.selectedIndexes()
        if not selected:
            return
        rows = sorted(list(set(index.row() for index in selected)))
        cols = sorted(list(set(index.column() for index in selected)))
        table_text = []
        for row in rows:
            row_data = []
            for col in cols:
                cell_data = table_view.model().index(row, col).data()
                row_data.append(str(cell_data) if cell_data is not None else "")
            table_text.append("\t".join(row_data))
        clipboard_text = "\n".join(table_text)
        try:
            if pyperclip:
                pyperclip.copy(clipboard_text)
        except (ImportError, getattr(pyperclip, 'PyperclipException', Exception) if pyperclip else Exception):
            QApplication.clipboard().setText(clipboard_text)
        QToolTip.showText(
            table_view.mapToGlobal(QPoint(0, 0)),
            "Copied to clipboard",
            table_view,
            table_view.rect(),
            2000,
        )

    # noinspection PyMethodMayBeStatic
    def show_context_menu(self, widget, position):
        menu = create_standard_edit_menu(widget)
        if menu.actions():
            menu.exec(widget.mapToGlobal(position))

# noinspection PyTypeChecker
class MainWindow(QWidget):
    def __init__(self, osu_api_client=None):
        super().__init__()
        self.results_window_instance = None
        self.current_user_data = None
        self.osu_api_client = osu_api_client
        self.config = {}
        self.icons = {}
        self.run_statistics = {}
        self.scan_completed = threading.Event()
        self.top_completed = threading.Event()
        self.img_completed = threading.Event()
        self.has_error = False
        self.overall_progress = 0
        self.current_task = "Ready to start"
        self.threadpool = QThreadPool()
        self.threadpool.setMaxThreadCount(GUI_THREAD_POOL_SIZE)

        self.background_pixmap = None
        self.scaled_background_pixmap = None
        self.title_label = None
        self.game_entry = None
        self.browse_button = None
        self.user_profile_widget: UserProfileWidget | None = None
        self.btn_all = None
        self.progress_bar = None
        self.status_label = None
        self.log_textbox = None
        self.results_button = None
        self.dev_label = None
        self.action_scan = None
        self.action_top = None
        self.action_img = None

        self.setWindowTitle("osu! Lost Scores Analyzer")
        self.setFixedSize(650, 800)
        self.setObjectName("mainWindow")
        
        config_dir = get_standard_dir("config")
        self.auth_manager = AuthManager(config_dir)
        self.oauth_flow = BrowserOAuthFlow(self.auth_manager)

        self.load_config()
        self.load_icons()
        self.load_background()
        self.init_ui()

        self._try_auto_detect_osu_path()
        self._try_auto_login()

    def enable_results_button(self):
        try:
            csv_files = [
                get_standard_dir("csv/lost_scores.csv"),
                get_standard_dir("csv/parsed_top.csv"),
                get_standard_dir("csv/top_with_lost.csv"),
            ]
            has_data = any(
                os.path.exists(f) and os.path.getsize(f) > 0 for f in csv_files
            )
            if self.results_button:
                self.results_button.setEnabled(has_data)
            logger.debug(
                "'See Full Results' button is %s", "enabled" if has_data else "disabled"
            )
        except Exception as e:
            logger.error(f"Error checking for results files: {e}")
            if self.results_button:
                self.results_button.setEnabled(False)

    def ensure_csv_files_exist(self):
        csv_dir = get_standard_dir("csv/")
        os.makedirs(csv_dir, exist_ok=True)

        files_to_create = {
            "lost_scores.csv": "PP,Beatmap ID,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,Rank\n",
            "parsed_top.csv": "PP,Beatmap ID,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,weight_%,weight_PP,Score ID,Rank\n",
            "top_with_lost.csv": "PP,Beatmap ID,Status,Beatmap,Mods,100,50,Misses,Accuracy,Score,Date,Rank,weight_%,weight_PP,Score ID\n",
        }

        for filename, header in files_to_create.items():
            path = os.path.join(csv_dir, filename)
            if not os.path.exists(path):
                try:
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(header)
                    self.append_log(f"Created empty file: {filename}", False)
                except Exception as e:
                    self.append_log(f"Error creating {filename}: {e}", False)

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

    def save_config(self):
        try:
            self.config["osu_path"] = self.game_entry.text().strip() if self.game_entry else ""

            if (self.current_user_data and self.user_profile_widget and
                    hasattr(self.user_profile_widget, "unranked_toggle")):
                self.config["username"] = self.current_user_data.get("username")
                
                try:
                    self.config["scores_count"] = (
                        self.user_profile_widget.scores_count_display.text()
                        if (hasattr(self.user_profile_widget, "scores_count_display") and
                            self.user_profile_widget.scores_count_display) else ""
                    )
                except RuntimeError:
                    self.config["scores_count"] = ""
                
                try:
                    self.config["include_unranked"] = (
                        self.user_profile_widget.unranked_toggle.isChecked()
                        if (hasattr(self.user_profile_widget, "unranked_toggle") and
                            self.user_profile_widget.unranked_toggle) else False
                    )
                except RuntimeError:
                    self.config["include_unranked"] = False
                
                try:
                    self.config["check_missing_ids"] = (
                        self.user_profile_widget.missing_id_toggle.isChecked()
                        if (hasattr(self.user_profile_widget, "missing_id_toggle") and
                            self.user_profile_widget.missing_id_toggle) else False
                    )
                except RuntimeError:
                    self.config["check_missing_ids"] = False
                
                try:
                    self.config["show_lost"] = (
                        self.user_profile_widget.show_lost_toggle.isChecked()
                        if (hasattr(self.user_profile_widget, "show_lost_toggle") and
                            self.user_profile_widget.show_lost_toggle) else False
                    )
                except RuntimeError:
                    self.config["show_lost"] = False

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
            db_close()
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
        event.accept()

    @Slot()
    def _on_results_window_closed(self):
        self.results_window_instance = None
        logger.debug("ResultsWindow closed and instance reference cleared")

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
        self.background_pixmap = None
        self.scaled_background_pixmap = None
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
                    orig_width = self.background_pixmap.width()
                    orig_height = self.background_pixmap.height()
                    self.scaled_background_pixmap = self.background_pixmap.scaled(
                        orig_width // 2,
                        orig_height // 2,
                        Qt.AspectRatioMode.IgnoreAspectRatio,
                        Qt.TransformationMode.SmoothTransformation,
                    )
                    logger.info("Background image loaded and scaled")
            except Exception as e:
                logger.error("Error loading background: %s", e)
                self.background_pixmap = None
                self.scaled_background_pixmap = None
        else:
            logger.warning(
                "Background file not found: %s",
                mask_path_for_log(os.path.normpath(BACKGROUND_IMAGE_PATH)),
            )

    def paintEvent(self, event):
        painter = QPainter(self)
        if hasattr(self, "scaled_background_pixmap") and self.scaled_background_pixmap:
            window_width = self.width()
            window_height = self.height()
            bg_width = self.scaled_background_pixmap.width()
            bg_height = self.scaled_background_pixmap.height()
            repeats_x = max(1, (window_width + bg_width - 1) // bg_width)
            repeats_y = max(1, (window_height + bg_height - 1) // bg_height)
            for y in range(repeats_y):
                for x in range(repeats_x):
                    painter.drawPixmap(
                        x * bg_width, y * bg_height, self.scaled_background_pixmap
                    )
        elif hasattr(self, "background_pixmap") and self.background_pixmap:
            painter.drawPixmap(self.rect(), self.background_pixmap)
        else:
            painter.fillRect(self.rect(), QCOLOR_SECONDARY_BG())
        painter.end()

    def init_ui(self):
        self.action_scan = QPushButton(self)
        self.action_scan.setVisible(False)
        self.action_scan.clicked.connect(self.start_scan)
        self.action_top = QPushButton(self)
        self.action_top.setVisible(False)
        self.action_top.clicked.connect(self.start_top)
        self.action_img = QPushButton(self)
        self.action_img.setVisible(False)
        self.action_img.clicked.connect(self.start_img)

        self.setLayout(None)  # type: ignore

        self.title_label = QLabel(self)
        self.title_label.setGeometry(50, 20, 550, 50)
        self.title_label.setObjectName("TitleLabel")
        self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.title_label.setText(
            f'<span style="color: {ACCENT_COLOR};">osu!</span><span style="color: {TEXT_PRIMARY};"> Lost Scores Analyzer</span> 🍋'
        )

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
        if self.config.get("osu_path"):
            self.game_entry.setText(self.config["osu_path"])

        self.browse_button = IconHoverButton(
            self.icons.get("folder", {}).get("normal"),
            self.icons.get("folder", {}).get("hover"),
            dir_container,
        )
        self.browse_button.setGeometry(510, 5, 30, 30)
        self.browse_button.clicked.connect(self.browse_directory)

        self.user_profile_widget = UserProfileWidget(self)
        self.user_profile_widget.setGeometry(50, 185, 550, 215)

        self.user_profile_widget.custom_keys_requested.connect(self.open_api_dialog)
        self.user_profile_widget.logout_requested.connect(self.logout_user)
        self.user_profile_widget.clear_cache_requested.connect(self.clear_app_cache)
        self.user_profile_widget.user_change_requested.connect(self.change_user)
        self.user_profile_widget.config_changed.connect(self.save_config)

        btn_y = 420
        self.btn_all = QPushButton("Start Scan", self)
        self.btn_all.setGeometry(50, btn_y, 550, 50)
        self.btn_all.setObjectName("btnAll")
        self.btn_all.clicked.connect(self.start_all_processes)

        self.progress_bar = AnimatedProgressBar(self)
        self.progress_bar.setGeometry(50, btn_y + 65, 550, 20)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)

        self.status_label = QLabel(self)
        self.status_label.setGeometry(50, btn_y + 90, 550, 25)
        self.status_label.setObjectName("StatusLabel")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        log_label = QLabel("Log", self)
        log_label.setGeometry(50, btn_y + 130, 550, 25)
        log_container = QFrame(self)
        log_container.setGeometry(50, btn_y + 160, 550, 120)
        log_container.setObjectName("LogContainer")
        log_layout = QVBoxLayout(log_container)
        log_layout.setContentsMargins(5, 5, 5, 5)

        self.log_textbox = QTextEdit(log_container)
        self.log_textbox.setReadOnly(True)
        log_layout.addWidget(self.log_textbox)

        self.results_button = QPushButton("See Full Results", self)
        self.results_button.setGeometry(50, btn_y + 290, 550, 40)
        self.results_button.setObjectName("resultsButton")
        self.results_button.clicked.connect(self.show_results_window)
        self.enable_results_button()

        self.dev_label = QLabel(
            '<a href="https://lemon4ik.kz/" style="color: gray; text-decoration: underline;">Developer Website</a>',
            self,
        )
        self.dev_label.setOpenExternalLinks(True)
        self.dev_label.adjustSize()
        self.dev_label.move(
            self.width() - self.dev_label.width() - 10,
            self.height() - self.dev_label.height() - 5,
        )

        self.status_label.setText(self.current_task)

    @Slot(str, bool)
    def append_log(self, message, update_last):
        try:
            if update_last:
                display_message = message

                self.current_task = display_message
                if self.status_label:
                    self.status_label.setText(display_message)
            else:
                current_time = datetime.now().strftime("[%H:%M:%S]")
                full_gui_message = f"{current_time} {message}\n"

                if self.log_textbox:
                    self.log_textbox.moveCursor(QtGui.QTextCursor.MoveOperation.End)
                    self.log_textbox.insertPlainText(full_gui_message)
                    self.log_textbox.ensureCursorVisible()

        except Exception as e:
            logger.error("Exception inside append_log for message '%s': %s", message, e)

    @Slot(int, int)
    def update_progress_bar(self, current, total):
        if total <= 0:
            return

        if self.scan_completed.is_set() and self.top_completed.is_set():
            progress = 85 + int((current / total) * 15)
        elif self.scan_completed.is_set():
            progress = 80 + int((current / total) * 5)
        else:
            progress = int((current / total) * 80)

        self.overall_progress = progress
        if self.progress_bar:
            self.progress_bar.setValue(progress)

    @Slot()
    def task_finished(self):
        logger.info("Replay scanning stage completed")
        if not self.scan_completed.is_set():
            if self.progress_bar:
                self.progress_bar.setValue(80)
            self.current_task = "Replay scanning stage completed"
            if self.status_label:
                self.status_label.setText(self.current_task)
            self.scan_completed.set()

    @Slot(str)
    def task_error(self, error_message):
        self.append_log(f"Task execution error: {error_message}", False)
        QMessageBox.warning(self, "Validation Error", f"An error occurred:\n{error_message}")

        if self.progress_bar:
            self.progress_bar.setValue(0)
        self.current_task = "Operation failed"
        if self.status_label:
            self.status_label.setText(self.current_task)

        self.has_error = True

        self.scan_completed.set()
        self.top_completed.set()
        self.img_completed.set()

        self.set_ui_busy(False)

    def browse_directory(self):
        folder = QFileDialog.getExistingDirectory(
            self, "Select osu! Game Directory", self.game_entry.text() if self.game_entry else ""
        )
        if folder and self.game_entry:
            self.game_entry.setText(folder.replace("/", os.sep))
            self.append_log(f"Selected folder: {mask_path_for_log(folder)}", False)
            self.save_config()

    def start_all_processes(self):
        game_dir = self.game_entry.text().strip() if self.game_entry else ""
        if not self.current_user_data:
            QMessageBox.warning(
                self,
                "Error",
                "Please log in first by providing API keys and a username",
            )
            return
        if not game_dir:
            QMessageBox.warning(self, "Error", "Please specify the osu! folder")
            return
        if not self.osu_api_client:
            QMessageBox.warning(
                self,
                "Error",
                "No API client configured. Please set up your API keys first",
            )
            return
        if not os.path.isdir(game_dir):
            QMessageBox.warning(
                self, "Error", f"Specified directory doesn't exist: {game_dir}"
            )
            return
        if not os.path.isdir(os.path.join(game_dir, "Songs")):
            QMessageBox.warning(
                self, "Error", f"Songs directory not found in: {game_dir}"
            )
            return
        if not os.path.isdir(os.path.join(game_dir, "Data", "r")):
            QMessageBox.warning(
                self, "Error", f"Replays directory (Data/r) not found in: {game_dir}"
            )
            return

        self.has_error = False
        self.set_ui_busy(True)
        if self.results_button:
            self.results_button.setEnabled(False)

        self.scan_completed.clear()
        self.top_completed.clear()
        self.img_completed.clear()

        self.overall_progress = 0
        if self.progress_bar:
            self.progress_bar.setValue(0)
        self.current_task = "Starting scan..."
        if self.status_label:
            self.status_label.setText(self.current_task)
        self.append_log("Starting analysis...", False)

        threading.Thread(target=self._run_sequence, daemon=True).start()

    def _run_sequence(self):
        stages = [
            ("scan_replays", self.action_scan, self.scan_completed, 1800),
            ("potential_top", self.action_top, self.top_completed, 900),
            ("image_creation", self.action_img, self.img_completed, 600),
        ]

        start_time = time.time()
        try:
            for name, action, event, timeout in stages:
                logger.info(f"Starting stage: {name}")
                if action:
                    action.click()

                # Give Qt event loop time to process any QTimer.singleShot calls
                import time as time_module
                time_module.sleep(0.1)
                
                event_was_set = event.wait(timeout)
                if self.has_error:
                    logger.error(
                        f"Error occurred during stage '{name}'. Aborting sequence"
                    )
                    return
                if not event_was_set:
                    logger.error(
                        f"Stage '{name}' timed out after {timeout} seconds. Aborting"
                    )
                    self.task_error(f"Stage '{name}' timed out")
                    return
                logger.info(f"Stage '{name}' completed")

            elapsed_time = time.time() - start_time
            if not self.has_error:
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "all_completed_successfully",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(float, elapsed_time),
                )
        except Exception as e:
            logger.exception("Error in the execution sequence:")
            self.task_error(f"Sequence error: {e}")
        finally:
            self.enable_all_button()

    def open_folder(self, path):
        try:
            if platform.system() == "Windows":
                os.startfile(path)
            elif platform.system() == "Darwin":
                subprocess.Popen(["open", path])
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception as e:
            logger.error(f"Could not open folder {path}: {e}")
            self.append_log(f"Error opening folder: {e}", False)

    @Slot(float)
    def all_completed_successfully(self, total_time):
        new_summary_data = load_summary_stats()
        if self.current_user_data:
            if self.user_profile_widget and hasattr(self.user_profile_widget, 'update_stats_display'):
                self.user_profile_widget.update_stats_display(
                    self.current_user_data, scan_data=new_summary_data
                )
        self.append_log("All operations completed successfully!", False)

        summary_lines = [
            "Analysis Summary:",
            f"- Time elapsed: {self.run_statistics.get('total_time_seconds', total_time):.2f} seconds",
            f"- Replays processed: {self.run_statistics.get('calculated_scores', 0)} / {self.run_statistics.get('total_replays', 0)}",
            f"- Lost scores found: {self.run_statistics.get('lost_scores_found', 0)} (from {self.run_statistics.get('lost_scores_pre_filter', 0)} candidates)",
        ]

        if self.run_statistics.get("maps_to_resolve", 0) > 0:
            summary_lines.append(
                f"- Missing maps resolved: {self.run_statistics.get('maps_resolved', 0)} / {self.run_statistics.get('maps_to_resolve', 0)}"
            )

        summary_report = "\n".join(summary_lines)
        self.append_log(summary_report, False)
        self.run_statistics.clear()

        self.enable_results_button()

        QtCore.QTimer.singleShot(100, self._show_completion_dialog)

        self.save_config()
        self.set_ui_busy(False)

    def _show_completion_dialog(self):
        
        try:
            self.show()
            self.raise_()
            self.activateWindow()
            
            QMessageBox.information(
                self,
                "Done",
                "Analysis completed! You can find results in the 'results' folder. Click 'See Full Results' to view detailed data.\n\nThe results folder will now be opened"
            )
            
            results_path = get_standard_dir("results")
            if os.path.exists(results_path) and os.path.isdir(results_path):
                self.append_log(
                    f"Opening results folder: {mask_path_for_log(results_path)}", False
                )
                self.open_folder(results_path)
                
        except Exception as e:
            logger.error(f"Error showing completion dialog: {e}")
            self.append_log(f"Error showing completion dialog: {e}", False)

    @Slot()
    def enable_all_button(self):
        if self.btn_all:
            self.btn_all.setDisabled(False)
        if self.browse_button:
            self.browse_button.setDisabled(False)
        if self.game_entry:
            self.game_entry.setReadOnly(False)
        if self.user_profile_widget:
            self.user_profile_widget.setDisabled(False)

    def start_scan(self):
        if not self.current_user_data:
            self.scan_completed.set()
            return

        game_dir = self.game_entry.text().strip() if self.game_entry else ""
        user_input = self.current_user_data["username"]
        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None:
            self.scan_completed.set()
            return

        if self.progress_bar:
            self.progress_bar.setValue(0)
        include_unranked = (self.user_profile_widget.unranked_toggle.isChecked()
                            if self.user_profile_widget and hasattr(self.user_profile_widget, 'unranked_toggle')
                               and self.user_profile_widget.unranked_toggle else False)
        check_missing_ids = (self.user_profile_widget.missing_id_toggle.isChecked()
                             if self.user_profile_widget and hasattr(self.user_profile_widget, 'missing_id_toggle')
                                and self.user_profile_widget.missing_id_toggle else False)

        worker = Worker(
            scan_replays,
            game_dir,
            identifier,
            lookup_key,
            include_unranked=include_unranked,
            check_missing_ids=check_missing_ids,
            osu_api_client=self.osu_api_client,
        )
        worker.signals.progress.connect(self.update_progress_bar)
        worker.signals.log.connect(self.append_log)
        worker.signals.result.connect(self.on_task_result)
        worker.signals.finished.connect(self.task_finished)
        worker.signals.error.connect(self.task_error)
        self.threadpool.start(worker)

    def start_top(self):
        if not self.current_user_data:
            self.top_completed.set()
            return

        game_dir = self.game_entry.text().strip() if self.game_entry else ""
        user_input = self.current_user_data["username"]
        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None or not self.osu_api_client:
            self.top_completed.set()
            return

        self.append_log("Generating potential top...", True)
        worker = Worker(
            make_top,
            game_dir,
            identifier,
            lookup_key,
            osu_api_client=self.osu_api_client,
            include_unranked=(self.user_profile_widget.unranked_toggle.isChecked()
                              if self.user_profile_widget and hasattr(self.user_profile_widget, 'unranked_toggle')
                                 and self.user_profile_widget.unranked_toggle else False),
        )
        worker.signals.log.connect(self.append_log)
        worker.signals.progress.connect(self.update_progress_bar)
        worker.signals.finished.connect(self.top_finished)
        worker.signals.error.connect(self.top_error)
        self.threadpool.start(worker)

    @Slot()
    def top_finished(self):
        logger.info("Potential top generation stage completed")
        if self.progress_bar:
            self.progress_bar.setValue(85)
        self.current_task = "Potential top generation stage completed"
        if self.status_label:
            self.status_label.setText(self.current_task)
        self.top_completed.set()

    @Slot(str)
    def top_error(self, error_message):
        self.append_log(f"Error creating top: {error_message}", False)
        QMessageBox.critical(
            self,
            "Error",
            f"An error occurred while creating top list:\n{error_message}",
        )
        if self.progress_bar:
            self.progress_bar.setValue(80)
        self.current_task = "Error creating top"
        if self.status_label:
            self.status_label.setText(self.current_task)
        self.has_error = True
        self.top_completed.set()

    def start_img(self):
        if not self.current_user_data or not self.osu_api_client:
            self.img_completed.set()
            return

        user_input = self.current_user_data["username"]
        identifier, lookup_key = self._parse_user_input(user_input)
        if identifier is None:
            self.img_completed.set()
            return

        try:
            scores_count_str = (self.user_profile_widget.scores_count_display.text()
                                if self.user_profile_widget and
                                   hasattr(self.user_profile_widget, 'scores_count_display') and
                                   self.user_profile_widget.scores_count_display else "10")
            scores_count = int(scores_count_str)
        except (ValueError, AttributeError):
            scores_count = 10

        show_lost = (self.user_profile_widget.show_lost_toggle.isChecked()
                     if self.user_profile_widget and hasattr(self.user_profile_widget, 'show_lost_toggle')
                        and self.user_profile_widget.show_lost_toggle else True)
        self.append_log("Generating images...", True)

        def task():
            try:

                def gui_log(message, update_last=False):
                    QtCore.QMetaObject.invokeMethod(
                        self,
                        "append_log",
                        QtCore.Qt.ConnectionType.QueuedConnection,
                        QtCore.Q_ARG(str, message),
                        QtCore.Q_ARG(bool, update_last),
                    )

                def update_progress(current, total):
                    QtCore.QMetaObject.invokeMethod(
                        self,
                        "update_progress_bar",
                        QtCore.Qt.ConnectionType.QueuedConnection,
                        QtCore.Q_ARG(int, current),
                        QtCore.Q_ARG(int, total),
                    )

                gui_log("Getting user data...", True)
                user_data = self.osu_api_client.user_osu(identifier, lookup_key) if self.osu_api_client else None
                if not user_data:
                    raise ValidationError(f"Failed to get user data for '{identifier}'")

                uid, uname = user_data["id"], user_data["username"]
                gui_log(f"User found: {uname} (ID: {uid})", False)
                update_progress(1, 4)

                gui_log("Creating lost scores image...", True)
                img_mod.make_img_lost(
                    user_id=uid,
                    user_name=uname,
                    max_scores=scores_count,
                    osu_api_client=self.osu_api_client,
                    gui_log=gui_log,
                )
                update_progress(2, 4)

                gui_log("Creating potential top image...", True)
                img_mod.make_img_top(
                    user_id=uid,
                    user_name=uname,
                    max_scores=scores_count,
                    show_lost=show_lost,
                    osu_api_client=self.osu_api_client,
                    gui_log=gui_log,
                )
                update_progress(4, 4)

                # Use the proper Qt threading mechanism
                QtCore.QMetaObject.invokeMethod(
                    self, "img_finished", QtCore.Qt.ConnectionType.QueuedConnection
                )
            except Exception as e:
                logger.exception("An exception occurred in the image generation thread")

                error_message = f"Error in image generation thread: {e}"
                QtCore.QMetaObject.invokeMethod(
                    self,
                    "img_error",
                    QtCore.Qt.ConnectionType.QueuedConnection,
                    QtCore.Q_ARG(str, error_message),
                )

        threading.Thread(target=task, daemon=True).start()

    @Slot()
    def img_finished(self):
        logger.info("Image creation stage completed")
        self.append_log("Images created (in 'results' folder)", False)
        if self.progress_bar:
            self.progress_bar.setValue(100)
        self.current_task = "Image creation stage completed"
        if self.status_label:
            self.status_label.setText(self.current_task)
        self.img_completed.set()


    @Slot(str)
    def img_error(self, error_message):
        self.append_log(f"Error generating images: {error_message}", False)
        QMessageBox.critical(
            self, "Image Generation Error", f"Failed to create images.\n{error_message}"
        )
        if self.progress_bar:
            self.progress_bar.setValue(85)
        self.current_task = "Error generating images"
        if self.status_label:
            self.status_label.setText(self.current_task)
        self.has_error = True
        self.img_completed.set()

    def _parse_user_input(self, user_input):
        user_input = user_input.strip()
        # noinspection HttpUrlsUsage
        if user_input.startswith(("http://", "https://")):
            try:
                parts = user_input.strip("/").split("/")
                if parts[-2] in ["users", "u"]:
                    identifier = parts[-1]
                else:
                    identifier = parts[-1]
            except IndexError:
                QMessageBox.warning(self, "Error", f"Invalid profile URL: {user_input}")
                return None, None
        else:
            identifier = user_input

        lookup_key = "id" if identifier.isdigit() else "username"
        return identifier, lookup_key

    # noinspection PyMethodMayBeStatic
    def show_context_menu(self, widget, position):
        menu = create_standard_edit_menu(widget)
        if menu.actions():
            menu.exec(widget.mapToGlobal(position))

    def show_results_window(self):
        try:
            if self.results_window_instance is not None:
                self.results_window_instance.close()
                self.results_window_instance = None
            
            self.results_window_instance = ResultsWindow(self)
            self.results_window_instance.finished.connect(
                self._on_results_window_closed
            )
            
            self.results_window_instance.show()
            self.results_window_instance.activateWindow()
            self.results_window_instance.raise_()
            
        except Exception as e:
            logger.error(f"Error showing results window: {e}")
            QMessageBox.critical(self, "Error", f"Failed to open results window: {e}")
            self.results_window_instance = None

    def _try_auto_detect_osu_path(self):
        if "osu_path" in self.config and os.path.isdir(self.config["osu_path"]):
            path = self.config["osu_path"]
            if self.game_entry:
                self.game_entry.setText(path.replace("/", os.sep))
            self.append_log(
                f"Loaded path from config: {mask_path_for_log(path)}", False
            )
            return

        if platform.system() == "Windows":
            local_app_data = os.getenv("LOCALAPPDATA")
            if local_app_data and os.path.isdir(os.path.join(local_app_data, "osu!")):
                path = os.path.join(local_app_data, "osu!")
                if self.game_entry:
                    self.game_entry.setText(path)
                self.append_log(
                    f"osu! folder auto-detected: {mask_path_for_log(path)}", False
                )
                self.save_config()
                return

        self.append_log(
            "osu! folder not found automatically. Please specify the path", False
        )

    def open_api_dialog(self):
        client_id, client_secret = OsuApiClient.get_keys_from_keyring()
        username = self.config.get("username", "")

        dialog = ApiDialog(self, client_id or "", client_secret or "", username)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            client_id = dialog.id_input.text().strip()
            client_secret = dialog.secret_input.text().strip()
            username = dialog.username_input.text().strip()

            self.set_ui_busy(True)

            self.append_log(f"Validating user '{username}' and API keys...", True)

            worker = Worker(self._validate_and_login, client_id, client_secret, username)

            worker.signals.result.connect(self._on_login_success)
            worker.signals.error.connect(self.task_error)
            worker.signals.log.connect(self.append_log)

            self.threadpool.start(worker)

    @Slot(str)
    def _on_login_error(self, error_message, context="initial_login"):
        self.append_log(f"Validation failed: {error_message}", False)
        if "User not found" in error_message and context == "user_change":
            QMessageBox.warning(
                self,
                "User Not Found",
                "Could not find the specified user. Please check the username or ID",
            )
        else:
            QMessageBox.warning(
                self,
                "Validation Failed",
                "Could not log in. Please check your Username, Client ID, and Secret",
            )

    def _validate_and_login(self, client_id, client_secret, username, gui_log):
        gui_log("Validating credentials...", True)
        self.osu_api_client = OsuApiClient.get_instance(client_id, client_secret)

        gui_log(f"Fetching profile for '{username}'...", True)
        user_identifier, lookup_key = self._parse_user_input(username)
        user_data = self.osu_api_client.user_osu(user_identifier, lookup_key) if self.osu_api_client else None

        if not user_data:
            raise ValidationError("User not found or API keys are invalid")

        gui_log("Credentials are valid. Ready to work!", True)
        if not OsuApiClient.save_keys_to_keyring(client_id, client_secret):
            gui_log("Warning: Failed to save API keys to system keyring", False)

        return user_data

    def logout_user(self):
        reply = QMessageBox.question(
            self,
            "Log Out",
            "Are you sure you want to log out?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        current_session = self.auth_manager.get_current_session()
        
        if current_session.auth_mode == AuthMode.OAUTH:
            self.auth_manager.clear_oauth_session_only()
            # Force invalidate cache to ensure clean state
            self.auth_manager._cached_session = None
            self.auth_manager._session_cache_valid = False
            # Reset OAuth browser state for clean logout/login cycle
            if hasattr(self, 'oauth_flow'):
                self.oauth_flow.reset_state()
            self.append_log("OAuth session successfully cleared", False)
        elif current_session.auth_mode == AuthMode.CUSTOM_KEYS:
            if OsuApiClient.delete_keys_from_keyring():
                self.append_log("API keys successfully removed from system keyring", False)
            else:
                self.append_log(
                    "Could not remove API keys from keyring (they might not have been saved)",
                    False,
                )
        else:
            self.append_log("No active session to logout", False)

        self.current_user_data = None
        OsuApiClient.reset_instance()
        self.osu_api_client = None

        if "username" in self.config:
            del self.config["username"]
        if "avatar_path" in self.config:
            del self.config["avatar_path"]
        self.save_config()

        if self.user_profile_widget and hasattr(self.user_profile_widget, 'set_to_logged_out_state'):
            self.user_profile_widget.set_to_logged_out_state()
        self.append_log("Successfully logged out", False)

    @Slot(object)
    def _on_login_success(self, user_data):
        self.current_user_data = user_data
        avatar_cache_path = os.path.join(
            get_standard_dir("cache/avatars"), f"avatar_{user_data['id']}.png"
        )

        self.config["avatar_path"] = avatar_cache_path
        self.config["username"] = user_data["username"]
        self.save_config()

        self.append_log(f"Successfully logged in as {user_data['username']}", False)
        if self.user_profile_widget and hasattr(self.user_profile_widget, 'update_state'):
            self.user_profile_widget.update_state(user_data, self.osu_api_client, self.config)
        self._download_avatar(user_data.get("avatar_url"), avatar_cache_path)

        potential_stats = load_summary_stats()
        if self.user_profile_widget and hasattr(self.user_profile_widget, 'update_stats_display'):
            self.user_profile_widget.update_stats_display(
                user_data, scan_data=potential_stats
            )

        self.set_ui_busy(False)

    def change_user(self, new_username):
        if not new_username or new_username == (
        self.current_user_data.get("username") if self.current_user_data else None):
            return

        client_id, client_secret = OsuApiClient.get_keys_from_keyring()
        worker = Worker(
            self._validate_and_login, client_id, client_secret, new_username
        )
        worker.signals.result.connect(self._on_login_success)
        worker.signals.error.connect(
            lambda msg: self._on_login_error(msg, context="user_change")
        )
        self.threadpool.start(worker)

    def _try_auto_login(self):
        session = self.auth_manager.get_current_session()
        self.append_log(f"Auto-login check: Found {session.auth_mode.value} session", True)
        
        if session.auth_mode == AuthMode.OAUTH and session.jwt_token:
            self.append_log(
                f"Found OAuth session for '{session.username}'. Attempting auto-login via backend...",
                False,
            )
            self.set_ui_busy(True)
            
            if not self.osu_api_client:
                self.osu_api_client = OsuApiClient.get_instance()
            self.osu_api_client.configure_for_oauth(session.jwt_token)
            self.osu_api_client = OsuApiClient.get_instance()
            
            worker = Worker(self._get_oauth_user_data)
            worker.signals.result.connect(self._on_oauth_login_success)
            worker.signals.error.connect(self._on_oauth_auto_login_error)
            worker.signals.log.connect(self.append_log)
            self.threadpool.start(worker)
            return
            
        elif session.auth_mode == AuthMode.CUSTOM_KEYS:
            client_id, client_secret = self.auth_manager.get_custom_keys()
            username = self.config.get("username")

            if client_id and client_secret and username:
                self.append_log(
                    f"Found saved custom keys for '{username}'. Trying to auto-login...",
                    False,
                )
                self.set_ui_busy(True)

                worker = Worker(
                    self._validate_and_login, client_id, client_secret, username
                )

                worker.signals.result.connect(self._on_login_success)
                worker.signals.error.connect(self.task_error)
                worker.signals.log.connect(self.append_log)

                self.threadpool.start(worker)

    def set_ui_busy(self, is_busy: bool):
        controls_enabled = not is_busy

        if self.btn_all:
            self.btn_all.setEnabled(controls_enabled)
        if self.browse_button:
            self.browse_button.setEnabled(controls_enabled)

        if self.game_entry:
            self.game_entry.setReadOnly(is_busy)

        if self.user_profile_widget:
            self.user_profile_widget.set_controls_enabled(controls_enabled)

    def _download_avatar(self, url, avatar_path):
        if not self.osu_api_client or not url or not avatar_path:
            return

        os.makedirs(os.path.dirname(avatar_path), exist_ok=True)
        worker = Worker(self.osu_api_client.download_image, url, avatar_path)
        if self.user_profile_widget and hasattr(self.user_profile_widget, 'set_avatar'):
            worker.signals.result.connect(self.user_profile_widget.set_avatar)
        self.threadpool.start(worker)

    def clear_app_cache(self):
        title = "Clear Application Cache"
        text = (
            "Are you sure you want to clear the cache and all generated reports?\n\n"
            "This is recommended for a full re-scan from scratch.\n\n"
            "Your main osu! 'Songs' folder and replays will NOT be affected"
        )

        reply = QMessageBox.question(
            self,
            title,
            text,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )

        if reply != QMessageBox.StandardButton.Yes:
            self.append_log("Cache clearing cancelled by user", False)
            return

        self.append_log("Starting cache and data cleanup...", False)
        self.append_log("Closing database connection before cleanup...", False)
        db_close()

        folders_to_clean = [CACHE_DIR, RESULTS_DIR, MAPS_DIR, CSV_DIR]
        removed_folders = []
        failed_folders = []

        for folder in folders_to_clean:
            if os.path.exists(folder):
                try:
                    shutil.rmtree(folder)
                    removed_folders.append(os.path.basename(folder))
                except (IOError, OSError):
                    logger.exception("Failed to remove cache directory: %s", folder)
                    failed_folders.append(os.path.basename(folder))

        if removed_folders:
            self.append_log(
                f"Successfully cleaned directories: {', '.join(removed_folders)}", False
            )

        for folder in folders_to_clean:
            try:
                os.makedirs(folder, exist_ok=True)
            except (IOError, OSError):
                logger.exception("Failed to re-create directory: %s", folder)
                failed_folders.append(os.path.basename(folder))

        if os.path.exists(DB_FILE):
            try:
                os.remove(DB_FILE)
                self.append_log("Removed database file", False)
            except (IOError, OSError):
                logger.exception("Failed to remove database file: %s", DB_FILE)
                failed_folders.append(os.path.basename(DB_FILE))

        db_init()
        file_parser.reset_in_memory_caches()
        if self.osu_api_client:
            self.osu_api_client.reset_caches()
        self.append_log("In-memory caches have been reset", False)

        if failed_folders:
            QMessageBox.warning(
                self,
                "Cleanup Error",
                f"Could not clean some items: {', '.join(failed_folders)}. See log for details",
            )
        else:
            QMessageBox.information(self, "Success", "All caches have been cleared")

        if self.current_user_data:
            if self.user_profile_widget and hasattr(self.user_profile_widget, 'update_stats_display'):
                self.user_profile_widget.update_stats_display(
                    self.current_user_data, scan_data=None
                )

        self.enable_results_button()

    @Slot(object)
    def on_task_result(self, result_dict):
        if isinstance(result_dict, dict):
            self.run_statistics.update(result_dict)

    def _on_oauth_login_clicked(self):
        self.append_log("Starting OAuth authorization...", False)
        self.set_ui_busy(True)
        
        if not self.oauth_flow.start_login():
            self.append_log("Failed to open browser for OAuth", False)
            self.set_ui_busy(False)
            return
        
        worker = Worker(self.oauth_flow.wait_for_session, 60)
        worker.signals.result.connect(self._on_oauth_complete)
        worker.signals.error.connect(lambda error_tuple: self._on_oauth_error(str(error_tuple)))
        self.threadpool.start(worker)

    def _on_oauth_complete(self, session):
        if session and session.auth_mode == AuthMode.OAUTH:
            self.append_log(f"OAuth authorization successful for user '{session.username}'", False)
            
            # Automatically bring application window back to focus after OAuth
            try:
                self.activateWindow()
                self.raise_()
                self.show()
                # Also focus the application at OS level
                self.activateWindow()
            except Exception as e:
                logger.debug(f"Could not restore window focus after OAuth: {e}")
            
            if not self.osu_api_client:
                self.osu_api_client = OsuApiClient.get_instance()
            self.osu_api_client.configure_for_oauth(session.jwt_token)
            self.osu_api_client = OsuApiClient.get_instance()
            
            worker = Worker(self._get_oauth_user_data)
            worker.signals.result.connect(self._on_oauth_login_success)
            worker.signals.error.connect(self.task_error)
            self.threadpool.start(worker)
        else:
            self._on_oauth_error("OAuth authorization failed or timeout")

    def _on_oauth_error(self, error_message):
        self.append_log(f"OAuth error: {error_message}", False)
        self.set_ui_busy(False)

    def _on_oauth_auto_login_error(self, error_message):
        self.append_log(f"OAuth auto-login failed: {error_message}", False)
        self.append_log("Backend server may be unavailable. Performing cleanup...", True)
        self.append_log("Clearing corrupted OAuth session and resetting API client...", True)
        
        OsuApiClient.reset_instance()
        self.osu_api_client = None
        
        self.auth_manager.clear_oauth_session_only()
        
        self.set_ui_busy(False)
        self.append_log("System reset complete. You can now use custom API keys or try OAuth again when backend is available.", False)

    def _get_oauth_user_data(self):
        try:
            if not self.osu_api_client:
                raise Exception("OAuth API client not initialized")
            user_data = self.osu_api_client.get_current_user_data()
            if not user_data:
                raise Exception("Could not get user data from OAuth API")
            return user_data
        except Exception as e:
            logger.error(f"Error getting OAuth user data: {e}")
            raise

    def _on_oauth_login_success(self, user_data):
        try:
            self.current_user_data = user_data
            
            avatar_cache_path = os.path.join(
                get_standard_dir("cache/avatars"), f"avatar_{user_data['id']}.png"
            )
            
            self.config["avatar_path"] = avatar_cache_path
            self.config["username"] = user_data["username"]
            
            if self.user_profile_widget:
                self.user_profile_widget._setup_logged_in_ui(user_data, self.config)
                
                potential_stats = load_summary_stats()
                self.user_profile_widget.update_stats_display(user_data, scan_data=potential_stats)
            
            self.save_config()
            
            self.append_log(f"Successfully logged in as '{user_data['username']}' via OAuth", False)
            
            if user_data.get("avatar_url"):
                self._download_avatar(user_data.get("avatar_url"), avatar_cache_path)
            
            self.set_ui_busy(False)
            
        except Exception as e:
            logger.error(f"Error in OAuth login success handler: {e}")
            self.append_log(f"Error setting up OAuth login: {str(e)}", False)
            self.set_ui_busy(False)

def create_gui(osu_api_client=None):
    app = QApplication.instance() or QApplication(sys.argv)

    font_path = get_standard_dir("assets/fonts")
    if os.path.isdir(font_path):
        font_db = QFontDatabase()
        fonts_loaded = 0
        for filename in os.listdir(font_path):
            if filename.lower().endswith((".ttf", ".otf")):
                if font_db.addApplicationFont(os.path.join(font_path, filename)) != -1:
                    fonts_loaded += 1
        if fonts_loaded > 0:
            logger.info(f"Loaded {fonts_loaded} local fonts")

    qss = load_qss()
    if qss:
        if hasattr(app, 'setStyleSheet'):
            app.setStyleSheet(qss)  # type: ignore
        logger.debug("QSS styles successfully applied to QApplication")
    else:
        logger.warning("QSS styles were not applied")

    window = MainWindow(osu_api_client)
    return window, app

if __name__ == "__main__":
    db_init()

    main_window, main_app = create_gui()
    main_window.show()

    show_api_limit_warning()

    exit_code = main_app.exec()
    db_close()
    sys.exit(exit_code)
