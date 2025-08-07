"""
Unified color constants for the pp-scam application.
This module provides both hex values for Qt/QSS and RGB tuples for PIL/image generation.
"""

from PySide6.QtGui import QColor

# Primary theme colors (hex format for Qt/QSS)
PRIMARY_BG = "#302444"  # Main background color
SECONDARY_BG = "#251a37"  # Secondary background (dialogs, containers)
BORDER_COLOR = "#4A3F5F"  # Default border color
ACCENT_COLOR = "#ee4bbd"  # Highlight/accent color (pink)
TEXT_PRIMARY = "#FFFFFF"  # Primary text color (white)
TEXT_SECONDARY = "#cccccc"  # Secondary text color (light gray)
TEXT_MUTED = "#A0A0A0"  # Muted text color (placeholders)
TEXT_DISABLED = "#666666"  # Disabled text color
USERNAME_COLOR = "#f0c4ff"  # Username display color (light purple)

# Status/feedback colors
SUCCESS_COLOR = "#4CAF50"  # Green for success states
ERROR_COLOR = "#E57373"  # Red for error states
WARNING_COLOR = "#FFC107"  # Yellow for warnings

# Specialized colors
STATS_TEXT = "#cccccc"  # Statistics text color
LINK_COLOR = "#ee4bbd"  # Link color (same as accent)
SEPARATOR_COLOR = "#cccccc"  # Text separators in stats


# PIL/Image generation colors (RGB tuples)
class ImageColors:
    """RGB color tuples for PIL image generation."""

    BG = (37, 26, 55)  # Background (corresponds to #251a37)
    CARD = (48, 36, 68)  # Card background (corresponds to #302444)
    CARD_LOST = (69, 34, 66)  # Lost scores card background
    WHITE = (255, 255, 255)  # White text
    HIGHLIGHT = (255, 153, 0)  # Orange highlight
    PP_SHAPE = (120, 50, 140)  # PP value background
    DATE = (200, 200, 200)  # Date text
    ACC = (255, 204, 33)  # Accuracy color
    WEIGHT = (255, 255, 255)  # Weight text
    GREEN = (128, 255, 128)  # Success/positive values
    RED = (255, 128, 128)  # Error/negative values
    USERNAME = (255, 204, 33)  # Username in images


# Qt Color objects (for programmatic use)
def get_qcolor(hex_color):
    """Convert hex color to QColor object."""
    return QColor(hex_color)


# Commonly used QColor objects
def get_qcolor_primary_bg():
    return get_qcolor(PRIMARY_BG)


def get_qcolor_secondary_bg():
    return get_qcolor(SECONDARY_BG)


def get_qcolor_accent():
    return get_qcolor(ACCENT_COLOR)


def get_qcolor_text_primary():
    return get_qcolor(TEXT_PRIMARY)


QCOLOR_PRIMARY_BG = get_qcolor_primary_bg
QCOLOR_SECONDARY_BG = get_qcolor_secondary_bg
QCOLOR_ACCENT = get_qcolor_accent
QCOLOR_TEXT_PRIMARY = get_qcolor_text_primary


# CSS class names for styled HTML elements
class CSSClasses:
    """CSS class names for styled HTML elements."""

    SEPARATOR = "text-separator"
    ERROR_TEXT = "error-text"
    LINK = "styled-link"
    APP_TITLE = "app-title"
    STATS_TEXT = "stats-text"
