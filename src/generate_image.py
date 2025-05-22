import csv
import datetime
import logging
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

from PIL import Image, ImageDraw, ImageFont

from config import CSV_DIR, MAP_DOWNLOAD_TIMEOUT, RESULTS_DIR
from database import db_get, db_save
from utils import get_resource_path, mask_path_for_log

logger = logging.getLogger(__name__)

FONTS_DIR = get_resource_path("assets/fonts")
GRADES_DIR = get_resource_path("assets/grades")
MODS_DIR = get_resource_path("assets/mod-icons")

try:
    TITLE_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Bold.otf"), 36)
    SUBTITLE_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Regular.otf"), 18)
    MAP_NAME_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Italic.otf"), 18)
    CREATOR_SMALL_FONT = ImageFont.truetype(
        os.path.join(FONTS_DIR, "Exo2-Italic.otf"), 13
    )
    VERSION_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Italic.otf"), 14)
    SMALL_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Regular.otf"), 16)
    BOLD_ITALIC_FONT = ImageFont.truetype(
        os.path.join(FONTS_DIR, "Exo2-BoldItalic.otf"), 18
    )
    BOLD_ITALIC_FONT_SMALL = ImageFont.truetype(
        os.path.join(FONTS_DIR, "Exo2-BoldItalic.otf"), 14
    )
except Exception:
    logger.exception("Failed to load Exo2 fonts, using default:")
    TITLE_FONT = SUBTITLE_FONT = MAP_NAME_FONT = CREATOR_SMALL_FONT = VERSION_FONT = (
        SMALL_FONT
    ) = ImageFont.load_default()
    BOLD_ITALIC_FONT = BOLD_ITALIC_FONT_SMALL = ImageFont.load_default()

os.makedirs(RESULTS_DIR, exist_ok=True)

CSV_LOST = os.path.join(CSV_DIR, "lost_scores.csv")
CSV_TOPLOST = os.path.join(CSV_DIR, "top_with_lost.csv")
IMG_LOST_OUT = os.path.join(RESULTS_DIR, "lost_scores_result.png")
IMG_TOP_OUT = os.path.join(RESULTS_DIR, "potential_top_result.png")

AVATAR_DIR = get_resource_path("assets/images/avatar")
COVER_DIR = get_resource_path("assets/images/cover")
os.makedirs(AVATAR_DIR, exist_ok=True)
os.makedirs(COVER_DIR, exist_ok=True)

CARD_HEIGHT = 60
CARD_SPACING = 2
TOP_PANEL_HEIGHT = 80
DEFAULT_MARGIN = 30
DEFAULT_BASE_CARD_WIDTH = 920
MOD_THRESHOLD_LOST = 4
MOD_THRESHOLD_TOP = 2
MOD_EXTENSION_WIDTH = 43

COLOR_BG = (37, 26, 55)
COLOR_CARD = (48, 36, 68)
COLOR_CARD_LOST = (69, 34, 66)
COLOR_WHITE = (255, 255, 255)
COLOR_HIGHLIGHT = (255, 153, 0)
PP_SHAPE_COLOR = (120, 50, 140)
DATE_COLOR = (200, 200, 200)
ACC_COLOR = (255, 204, 33)
WEIGHT_COLOR = (255, 255, 255)
GREEN_COLOR = (128, 255, 128)
RED_COLOR = (255, 128, 128)
USERNAME_COLOR = (255, 204, 33)
CARD_CORNER_RADIUS = 15
GRADE_TARGET_WIDTH = 45
PP_SHAPE_WIDTH = 100
MODS_EDGE_OFFSET = 90
ACCURACY_COLUMN_WIDTH = 120
PP_COLUMN_WIDTH = 70
VERTICAL_TEXT_SPACING = 5
MODS_RIGHT_MARGIN = 10
MOD_ICON_MAX_SIZE = 38
MOD_ICON_SPACING = 5


def create_placeholder_image(filename, username, message):
    width, height = 920, 400
    img = Image.new("RGBA", (width, height), COLOR_BG)
    draw = ImageDraw.Draw(img)

    draw.text(
        (width // 2, 50),
        "osu! Lost Scores Analyzer",
        font=TITLE_FONT,
        fill=ACC_COLOR,
        anchor="mm",
    )

    draw.text(
        (width // 2, 100),
        f"Player: {username}",
        font=SUBTITLE_FONT,
        fill=USERNAME_COLOR,
        anchor="mm",
    )

    draw.text(
        (width // 2, height // 2),
        message,
        font=SUBTITLE_FONT,
        fill=COLOR_WHITE,
        anchor="mm",
    )

    draw.text(
        (width // 2, height - 50),
        "Try running the analysis again or check for missing files",
        font=SMALL_FONT,
        fill=DATE_COLOR,
        anchor="mm",
    )

    out_path = get_resource_path(os.path.join("results", filename))

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    img.save(out_path)
    logger.info(
        "Placeholder image saved to %s", mask_path_for_log(os.path.normpath(out_path))
    )


def short_mods(mods_str):
    mlist = [m.strip() for m in mods_str.split(",") if m.strip()]
    return [m for m in mlist if m.upper() not in {"CL", "NM"}]


def short_txt(text, max_len=50):
    return text if len(text) <= max_len else text[: max_len - 3] + "..."


def since_date(date_str):
    try:
        dt = datetime.datetime.strptime(date_str, "%d-%m-%Y %H-%M-%S")
    except ValueError:
        return "Unknown date"
    now = datetime.datetime.now()
    ddays = (now - dt).days
    if ddays < 0:
        return "Unknown date"
    months = ddays / 30
    if months < 1.5:
        return "about a month ago"
    if months < 11.5:
        return f"{int(months + 0.5)} months ago"
    yrs = months / 12
    if yrs < 1.5:
        return "a year ago"
    y_rounded = int(yrs + 0.5)
    if y_rounded == 1:
        return "a year ago"
    elif y_rounded == 2:
        return "two years ago"
    else:
        return f"{y_rounded} years ago"


def download_and_draw_avatar(
    d,
    user_id,
    user_name,
    avatar_url,
    x,
    y,
    size,
    osu_api_client=None,
    gui_log=None,
    avatar_radius=15,
    placeholder_color=(80, 80, 80, 255),
):
    if not osu_api_client:
        logger.warning("No API client provided for downloading avatar")
        d.rounded_rectangle(
            (x, y, x + size, y + size), radius=avatar_radius, fill=placeholder_color
        )
        return None, False

    avatar_filename = f"avatar_{user_name}.png"
    avatar_path = os.path.join(AVATAR_DIR, avatar_filename)

    if avatar_url:
        try:
            if gui_log:
                gui_log(f"Downloading avatar for user {user_name}", update_last=True)
            osu_api_client.download_image(avatar_url, avatar_path, MAP_DOWNLOAD_TIMEOUT)

            avatar_img_raw = (
                Image.open(avatar_path).convert("RGBA").resize((size, size))
            )
            av_mask = Image.new("L", (size, size), 0)
            ImageDraw.Draw(av_mask).rounded_rectangle(
                (0, 0, size, size), radius=avatar_radius, fill=255
            )
            avatar_img_raw.putalpha(av_mask)
            return avatar_img_raw, True
        except FileNotFoundError:
            logger.warning(
                f"Avatar file {mask_path_for_log(avatar_path)} not found after download attempt"
            )
        except Exception as img_err:
            logger.warning(
                f"Error processing avatar {mask_path_for_log(avatar_path)}: {img_err}"
            )

    logger.warning(f"Using placeholder for avatar user_id {user_id}")
    d.rounded_rectangle(
        (x, y, x + size, y + size), radius=avatar_radius, fill=placeholder_color
    )
    return None, False


def get_beatmap_metadata(
    beatmap_id, beatmap_full_name, metadata_cache=None, osu_api_client=None
):
    try:
        if not osu_api_client:
            raise ValueError("API client not provided")

        if metadata_cache is None:
            metadata_cache = {}

        if beatmap_id in metadata_cache:
            return metadata_cache[beatmap_id]

        raw_artist = ""
        raw_title = ""
        creator = ""
        version = ""
        cover_url = None
        beatmapset_id = None

        if beatmap_full_name:
            try:
                pattern = r"(.+) - (.+) \((.+)\) \[(.+)\]"
                match = re.match(pattern, beatmap_full_name)
                if match:
                    raw_artist, raw_title, creator, version = match.groups()
            except Exception as e:
                logger.warning(
                    f"Failed to parse beatmap name '{beatmap_full_name}': {e}"
                )

                                           
        if beatmap_id and beatmap_id.isdigit():
            try:
                db_info = db_get(beatmap_id)
                if db_info:
                    raw_artist = db_info.get("artist", raw_artist) or raw_artist
                    raw_title = db_info.get("title", raw_title) or raw_title
                    creator = db_info.get("creator", creator) or creator
                    version = db_info.get("version", version) or version
                    beatmapset_id = db_info.get("beatmapset_id")

                                                                                      
                    if beatmapset_id:
                                                               
                        cover_file = os.path.join(
                            COVER_DIR, f"cover_set_{beatmapset_id}.png"
                        )
                        if os.path.exists(cover_file):
                            cover_url = "local"
                        else:
                                                            
                            cover_url = f"https://assets.ppy.sh/beatmaps/{beatmapset_id}/covers/cover@2x.jpg"
                    else:
                                                                               
                        old_cover_file = os.path.join(
                            COVER_DIR, f"cover_{beatmap_id}.png"
                        )
                        if os.path.exists(old_cover_file):
                            cover_url = "local"
            except Exception as e:
                logger.warning(
                    f"Error getting map data from DB for ID {beatmap_id}: {e}"
                )

                                                                             
        if all([raw_artist, raw_title, creator, version]) and cover_url:
            result = {
                "artist": raw_artist,
                "title": raw_title,
                "creator": creator,
                "version": version,
                "cover_url": cover_url,
                "beatmapset_id": beatmapset_id,
            }
            metadata_cache[beatmap_id] = result
            return result

                                                   
        if beatmap_id and beatmap_id.isdigit():
            if beatmap_id in metadata_cache:
                data = metadata_cache[beatmap_id]
                raw_artist = data.get("artist", raw_artist) or raw_artist
                raw_title = data.get("title", raw_title) or raw_title
                creator = data.get("creator", creator) or creator
                version = data.get("version", version) or version
                cover_url = data.get("cover_url")
                beatmapset_id = data.get("beatmapset_id", beatmapset_id)
            else:
                                                     
                need_api_request = not all(
                    [raw_artist, raw_title, creator, version]
                ) or (not cover_url and not beatmapset_id)

                if need_api_request:
                    try:
                        logger.debug("Getting beatmap data for ID %s", beatmap_id)
                        bdata = osu_api_client.map_osu(beatmap_id)
                        if bdata:
                            raw_artist = bdata.get("artist") or raw_artist
                            raw_title = bdata.get("title") or raw_title
                            creator = bdata.get("creator") or creator
                            version = bdata.get("version") or version

                            beatmapset = bdata.get("beatmapset", {})
                            if beatmapset:
                                new_beatmapset_id = beatmapset.get("id")
                                if new_beatmapset_id:
                                    beatmapset_id = new_beatmapset_id

                                                                    
                                    try:
                                        db_save(
                                            beatmap_id,
                                            bdata.get("status", "unknown"),
                                            raw_artist,
                                            raw_title,
                                            version,
                                            creator,
                                            bdata.get("hit_objects", 0),
                                            beatmapset_id,
                                        )
                                    except Exception as db_error:
                                        logger.warning(
                                            f"Failed to save beatmapset_id to database: {db_error}"
                                        )

                                                                    
                                    if "covers" in beatmapset:
                                        cover_url = beatmapset["covers"].get("cover@2x")
                                    else:
                                        cover_url = f"https://assets.ppy.sh/beatmaps/{beatmapset_id}/covers/cover@2x.jpg"

                                    logger.debug(
                                        "Retrieved beatmapset_id %s and cover URL for beatmap %s",
                                        beatmapset_id,
                                        beatmap_id,
                                    )
                    except Exception as e:
                        logger.warning(
                            "Error getting map data for ID %s from API: %s",
                            beatmap_id,
                            e,
                        )
    except Exception as e:
        logger.error(f"Error in get_beatmap_metadata for ID {beatmap_id}: {e}")

    result = {
        "artist": raw_artist,
        "title": raw_title,
        "creator": creator,
        "version": version,
        "cover_url": cover_url,
        "beatmapset_id": beatmapset_id,
    }
    if beatmap_id:
        metadata_cache[beatmap_id] = result
    return result


def get_and_draw_cover(
    beatmap_id,
    cover_url,
    width,
    height,
    osu_api_client=None,
    gui_log=None,
    beatmapset_id=None,
):
    if not osu_api_client:
        logger.warning("No API client provided for downloading cover")
        c_img = Image.new("RGBA", (width, height), (80, 80, 80, 255))
        return c_img

    c_img = None
    cover_file = None

                                                                  
    if beatmapset_id:
        cover_file = os.path.join(COVER_DIR, f"cover_set_{beatmapset_id}.png")
                                                                
        if os.path.exists(cover_file):
            try:
                c_img = Image.open(cover_file).convert("RGBA").resize((width, height))
                logger.debug(
                    f"Using beatmapset cover from {mask_path_for_log(cover_file)}"
                )
                return c_img
            except Exception as cover_err:
                logger.warning(
                    f"Failed to open beatmapset cover {mask_path_for_log(cover_file)}: {cover_err}"
                )
                c_img = None

                                                                                      
    old_cover_file = None
    if beatmap_id and beatmap_id.isdigit():
        old_cover_file = os.path.join(COVER_DIR, f"cover_{beatmap_id}.png")
        if os.path.exists(old_cover_file):
            try:
                c_img = (
                    Image.open(old_cover_file).convert("RGBA").resize((width, height))
                )
                logger.debug(
                    f"Using beatmap cover from {mask_path_for_log(old_cover_file)}"
                )

                                                                                                  
                if beatmapset_id and not os.path.exists(cover_file):
                    try:
                        shutil.copy2(old_cover_file, cover_file)
                        logger.debug(
                            f"Copied cover from {mask_path_for_log(old_cover_file)} to {mask_path_for_log(cover_file)}"
                        )
                    except Exception as copy_err:
                        logger.warning(f"Failed to copy cover file: {copy_err}")

                return c_img
            except Exception as cover_err:
                logger.warning(
                    f"Failed to open beatmap cover {mask_path_for_log(old_cover_file)}: {cover_err}"
                )
                c_img = None

                                                                                     
    if not c_img:
                             
        c_img = Image.new("RGBA", (width, height), (80, 80, 80, 255))

    return c_img


def preload_cover_images(
    rows, metadata_cache=None, osu_api_client=None, gui_log=None, max_workers=8
):
           
    if not osu_api_client:
        logger.warning("No API client provided for preloading covers")
        return

    if metadata_cache is None:
        metadata_cache = {}

    cover_urls_to_download = {}                                         
    covers_to_download = []                                                                         

                                                                        
    for row in rows:
        beatmap_id = row.get("Beatmap ID", "").strip()
        beatmap_full_name = row.get("Beatmap", "")

        if not beatmap_id:
            continue

        metadata = get_beatmap_metadata(
            beatmap_id, beatmap_full_name, metadata_cache, osu_api_client
        )

        beatmapset_id = metadata.get("beatmapset_id")
        cover_url = metadata.get("cover_url")

        if not cover_url or cover_url == "local":
            continue

                                         
        if beatmapset_id:
            target_file = os.path.join(COVER_DIR, f"cover_set_{beatmapset_id}.png")
            key = f"set_{beatmapset_id}"
        else:
            target_file = os.path.join(COVER_DIR, f"cover_{beatmap_id}.png")
            key = f"map_{beatmap_id}"

                                                   
        if os.path.exists(target_file):
            continue

                                                                                
        if key in cover_urls_to_download:
            continue

        cover_urls_to_download[key] = cover_url
        covers_to_download.append((beatmap_id, beatmapset_id, cover_url, target_file))

                                                  
    if not covers_to_download:
        logger.info("No covers need to be downloaded")
        return

    if gui_log:
        gui_log(
            f"Downloading cover images: 0/{len(covers_to_download)}",
            update_last=True,
        )

    logger.info(
        f"Preloading {len(covers_to_download)} cover images ({max_workers} workers)"
    )

                                        
    def download_single_cover(item):
        beatmap_id, beatmapset_id, cover_url, target_file = item
        try:
                                                    
            os.makedirs(os.path.dirname(target_file), exist_ok=True)

                               
            success = osu_api_client.download_image(
                cover_url, target_file, MAP_DOWNLOAD_TIMEOUT
            )
            identifier = beatmapset_id if beatmapset_id else beatmap_id
            id_type = "beatmapset" if beatmapset_id else "beatmap"

            if success:
                logger.debug(
                    f"Successfully downloaded cover for {id_type} {identifier}"
                )
            else:
                logger.warning(f"Failed to download cover for {id_type} {identifier}")

            return success
        except Exception as e:
            logger.error(f"Error downloading cover for beatmap_id {beatmap_id}: {e}")
            return False

                                     
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_single_cover, item): item
            for item in covers_to_download
        }

        completed = 0
        for future in as_completed(futures):
            completed += 1
            if gui_log:
                gui_log(
                    f"Downloading cover images: {completed}/{len(covers_to_download)}...",
                    update_last=True,
                )

    if gui_log:
        gui_log(
            f"Finished downloading {len(covers_to_download)} cover images",
            update_last=True,
        )

    logger.info(f"Completed preloading {len(covers_to_download)} cover images")


def _prepare_card_background(
    card_w,
    card_h,
    is_lost_row,
    show_weights,
    beatmap_id,
    cover_url,
    osu_api_client=None,
    gui_log=None,
    beatmapset_id=None,
):
    if not osu_api_client:
        raise ValueError("API client not provided")

    bg_color = COLOR_CARD_LOST if show_weights and is_lost_row else COLOR_CARD
    bg_img = Image.new("RGBA", (card_w, card_h), bg_color)

    cover_w = card_w // 3
    cover_h = card_h
    c_img = get_and_draw_cover(
        beatmap_id,
        cover_url,
        cover_w,
        cover_h,
        osu_api_client,
        gui_log=gui_log,
        beatmapset_id=beatmapset_id,
    )

    fade_mask = Image.new("L", (cover_w, cover_h), 255)
    dm_fade = ImageDraw.Draw(fade_mask)
    for x in range(cover_w):
        alpha_val = int(90 - (x / cover_w) * 90)
        dm_fade.line([(x, 0), (x, cover_h)], fill=alpha_val)
    bg_img.paste(c_img, (0, 0), fade_mask)

    return bg_img


def _draw_grade_icon(base, d_card, grade, card_x, center_line):
    grade_icon_path = os.path.join(GRADES_DIR, f"{grade}.png")
    if os.path.isfile(grade_icon_path):
        try:
            g_img = Image.open(grade_icon_path).convert("RGBA")
            ow, oh = g_img.size
            scale = GRADE_TARGET_WIDTH / ow
            nw, nh = int(ow * scale), int(oh * scale)
            g_img_resized = g_img.resize((nw, nh), Image.Resampling.LANCZOS)
            base.paste(
                g_img_resized, (card_x + 10, center_line - nh // 2), g_img_resized
            )
            return True
        except Exception as grade_err:
            logger.warning(
                f"Error processing grade icon {mask_path_for_log(grade_icon_path)}: {grade_err}"
            )

    d_card.text(
        (card_x + 10, center_line - 8), grade, font=SUBTITLE_FONT, fill=COLOR_WHITE
    )
    return False


def _draw_beatmap_info(
    d_card,
    raw_title,
    raw_artist,
    creator,
    version,
    date_str,
    text_x,
    text_y_map,
    card_y,
):
    full_name = short_txt(f"{raw_title} by {raw_artist}", 50)
    d_card.text((text_x, text_y_map), full_name, font=MAP_NAME_FONT, fill=COLOR_WHITE)
    text_y_map += 20

    d_card.text(
        (text_x, text_y_map), f"by {creator}", font=CREATOR_SMALL_FONT, fill=COLOR_WHITE
    )
    text_y_map += 16

    date_human = since_date(date_str)
    gap = "    "
    try:
        version_bbox = d_card.textbbox((0, 0), version, font=VERSION_FONT)
        version_w = version_bbox[2] - version_bbox[0]
        gap_bbox = d_card.textbbox((0, 0), gap, font=VERSION_FONT)
        gap_w = gap_bbox[2] - gap_bbox[0]
        d_card.text(
            (text_x, text_y_map), version, font=VERSION_FONT, fill=COLOR_HIGHLIGHT
        )
        d_card.text(
            (text_x + version_w + gap_w, text_y_map),
            date_human,
            font=VERSION_FONT,
            fill=DATE_COLOR,
        )
    except AttributeError:
        d_card.text(
            (text_x, text_y_map),
            f"{version}{gap}{date_human}",
            font=VERSION_FONT,
            fill=DATE_COLOR,
        )


def _draw_pp_section(d_card, row, card_x, card_y, card_w, card_h, center_line):
    shape_w = PP_SHAPE_WIDTH
    shape_left = card_x + card_w - shape_w

    d_card.rounded_rectangle(
        (shape_left, card_y, shape_left + shape_w, card_y + card_h),
        radius=CARD_CORNER_RADIUS,
        fill=PP_SHAPE_COLOR,
    )

    raw_pp = row.get("PP", "0")
    try:
        pp_val = round(float(raw_pp))
    except ValueError:
        pp_val = 0
    pp_str = f"{pp_val}pp"

    try:
        box_pp = d_card.textbbox((0, 0), pp_str, font=SUBTITLE_FONT)
        w_pp_ = box_pp[2] - box_pp[0]
        h_pp_ = box_pp[3] - box_pp[1]
        manual_offset_pp = -4
        text_x_pp = shape_left + shape_w / 2 - w_pp_ / 2
        text_y_pp = center_line - h_pp_ / 2 + manual_offset_pp
        d_card.text(
            (text_x_pp, text_y_pp), pp_str, font=SUBTITLE_FONT, fill=COLOR_WHITE
        )
    except AttributeError:
        d_card.text(
            (shape_left + 15, center_line - 10),
            pp_str,
            font=SUBTITLE_FONT,
            fill=COLOR_WHITE,
        )

    return shape_left


def draw_score_card(
    base,
    d_card,
    row,
    card_x,
    card_y,
    card_w,
    card_h,
    is_lost_row=False,
    show_weights=False,
    metadata_cache=None,
    osu_api_client=None,
    gui_log=None,
):
    if not osu_api_client:
        raise ValueError("API client not provided")

    center_line = card_y + card_h // 2

    beatmap_id = row.get("Beatmap ID", "").strip()
    beatmap_full_name = row.get("Beatmap", "")

    if metadata_cache is None:
        metadata_cache = {}
    metadata = get_beatmap_metadata(
        beatmap_id, beatmap_full_name, metadata_cache, osu_api_client
    )

    raw_artist = metadata["artist"]
    raw_title = metadata["title"]
    creator = metadata["creator"]
    version = metadata["version"]
    cover_url = metadata["cover_url"]
    beatmapset_id = metadata.get("beatmapset_id")

    bg_img = _prepare_card_background(
        card_w,
        card_h,
        is_lost_row,
        show_weights,
        beatmap_id,
        cover_url,
        osu_api_client,
        gui_log,
        beatmapset_id=beatmapset_id,
    )

    corner_mask = Image.new("L", (card_w, card_h), 0)
    dr_corner = ImageDraw.Draw(corner_mask)
    dr_corner.rounded_rectangle(
        (0, 0, card_w, card_h), radius=CARD_CORNER_RADIUS, fill=255
    )
    base.paste(bg_img, (card_x, card_y), corner_mask)

    grade = row.get("Rank", "?")
    _draw_grade_icon(base, d_card, grade, card_x, center_line)

    text_x = card_x + 70
    text_y_map = card_y + 4
    date_str = row.get("Date", "")
    _draw_beatmap_info(
        d_card,
        raw_title,
        raw_artist,
        creator,
        version,
        date_str,
        text_x,
        text_y_map,
        card_y,
    )

    shape_left = _draw_pp_section(
        d_card, row, card_x, card_y, card_w, card_h, center_line
    )
    right_block_x = shape_left - 20

    if not show_weights:
        draw_accuracy_and_mods_lost(
            d_card, base, row, right_block_x, center_line, shape_left
        )
    else:
        draw_weighted_info(d_card, base, row, shape_left, center_line)


def _format_accuracy_text(accuracy_value):
    try:
        return f"{float(accuracy_value):.2f}%"
    except ValueError:
        return f"{accuracy_value}%" if accuracy_value else "?.??%"


def draw_accuracy_and_mods_lost(
    d_card, base, row, right_block_x, center_line, shape_left
):
    mods_edge = right_block_x - MODS_EDGE_OFFSET
    acc_center_x = (mods_edge + shape_left) / 2

    raw_acc_str = row.get("Accuracy", "0")
    acc_s = _format_accuracy_text(raw_acc_str)

    try:
        acc_box = d_card.textbbox((0, 0), acc_s, font=BOLD_ITALIC_FONT)
        d_card.text(
            (acc_center_x, center_line),
            acc_s,
            font=BOLD_ITALIC_FONT,
            fill=ACC_COLOR,
            anchor="mm",
        )
    except AttributeError:
        acc_box = d_card.textbbox((0, 0), acc_s, font=BOLD_ITALIC_FONT)
        if acc_box:
            acc_w = acc_box[2] - acc_box[0]
            d_card.text(
                (acc_center_x - acc_w / 2, center_line - 10),
                acc_s,
                font=BOLD_ITALIC_FONT,
                fill=ACC_COLOR,
            )
        else:
            d_card.text(
                (acc_center_x - 30, center_line - 10),
                acc_s,
                font=BOLD_ITALIC_FONT,
                fill=ACC_COLOR,
            )

    draw_mods(d_card, base, row, mods_edge, center_line)


def draw_weighted_info(d_card, base, row, shape_left, center_line):
    wpp_x = shape_left - 10
    raw_wpp = row.get("weight_PP", "")
    try:
        weight_pp_text = f"{round(float(raw_wpp))}pp"
    except ValueError:
        weight_pp_text = ""

    if weight_pp_text:
        try:
            d_card.text(
                (wpp_x - PP_COLUMN_WIDTH / 2, center_line),
                weight_pp_text,
                font=BOLD_ITALIC_FONT_SMALL,
                fill=WEIGHT_COLOR,
                anchor="mm",
            )
        except AttributeError:
            d_card.text(
                (wpp_x - PP_COLUMN_WIDTH + 5, center_line - 8),
                weight_pp_text,
                font=BOLD_ITALIC_FONT_SMALL,
                fill=WEIGHT_COLOR,
            )

    acc_block_x = wpp_x - PP_COLUMN_WIDTH - ACCURACY_COLUMN_WIDTH / 2

    raw_acc = row.get("Accuracy", "0")
    acc_str2 = _format_accuracy_text(raw_acc)

    raw_wpercent = row.get("weight_%", "")
    try:
        w_percent_str = f"weighted {round(float(raw_wpercent))}%"
    except ValueError:
        w_percent_str = ""

    try:
        acc_box = d_card.textbbox((0, 0), acc_str2, font=BOLD_ITALIC_FONT)
        acc_h = acc_box[3] - acc_box[1]

        left_align_x = acc_block_x - ACCURACY_COLUMN_WIDTH / 2 + 10

        d_card.text(
            (left_align_x, center_line - acc_h / 2 - VERTICAL_TEXT_SPACING),
            acc_str2,
            font=BOLD_ITALIC_FONT,
            fill=ACC_COLOR,
            anchor="lm",
        )

        if w_percent_str:
            wpct_box = d_card.textbbox((0, 0), w_percent_str, font=CREATOR_SMALL_FONT)
            wpct_h = wpct_box[3] - wpct_box[1]

            d_card.text(
                (left_align_x, center_line + wpct_h / 2 + VERTICAL_TEXT_SPACING),
                w_percent_str,
                font=CREATOR_SMALL_FONT,
                fill=WEIGHT_COLOR,
                anchor="lm",
            )
    except AttributeError:
        d_card.text(
            (acc_block_x - ACCURACY_COLUMN_WIDTH / 2 + 10, center_line - 14),
            acc_str2,
            font=BOLD_ITALIC_FONT,
            fill=ACC_COLOR,
        )
        if w_percent_str:
            d_card.text(
                (acc_block_x - ACCURACY_COLUMN_WIDTH / 2 + 10, center_line + 6),
                w_percent_str,
                font=CREATOR_SMALL_FONT,
                fill=WEIGHT_COLOR,
            )

    mods_right_edge = acc_block_x - ACCURACY_COLUMN_WIDTH / 2 - MODS_RIGHT_MARGIN
    draw_mods(d_card, base, row, mods_right_edge, center_line)


def draw_mods(d_card, base, row, mods_right_edge, center_line):
    mods_list = short_mods(row.get("Mods", ""))
    mod_x_cur = mods_right_edge

    for m_ in reversed(mods_list):
        path_ = os.path.join(MODS_DIR, f"{m_.upper()}.png")
        if os.path.isfile(path_):
            try:
                mg = Image.open(path_).convert("RGBA")
                ow, oh = mg.size
                sc = min(MOD_ICON_MAX_SIZE / ow, MOD_ICON_MAX_SIZE / oh)
                nw, nh = int(ow * sc), int(oh * sc)
                mod_x_cur -= nw
                mod_img_resized = mg.resize((nw, nh), Image.Resampling.LANCZOS)
                base.paste(
                    mod_img_resized,
                    (int(mod_x_cur), center_line - nh // 2),
                    mod_img_resized,
                )
                mod_x_cur -= MOD_ICON_SPACING
            except Exception as mod_err:
                logger.warning(
                    f"Error processing mod icon {mask_path_for_log(path_)}: {mod_err}"
                )
        else:
            try:
                box_m = d_card.textbbox((0, 0), m_, font=SMALL_FONT)
                w_m = box_m[2] - box_m[0]
                mod_x_cur -= w_m
                d_card.text(
                    (mod_x_cur, center_line - 8),
                    m_,
                    font=SMALL_FONT,
                    fill=COLOR_WHITE,
                )
                mod_x_cur -= MOD_ICON_SPACING
            except AttributeError:
                pass


def draw_header(
    base,
    d,
    width,
    margin,
    title,
    username,
    username_color,
    user_json,
    av_size,
    baseline_y,
    title_h,
    extra_shift=0,
    osu_api_client=None,
):
    d.text((margin, baseline_y), title, font=TITLE_FONT, fill=COLOR_WHITE)
    try:
        title_box = d.textbbox((margin, baseline_y), title, font=TITLE_FONT)
        title_right_x = title_box[2]
        title_h = title_box[3] - title_box[1]
    except AttributeError:
        title_right_x = margin + 200
        title_h = 40

    av_x = width - margin - av_size
    center_y = baseline_y + title_h / 2
    av_y = int(center_y - av_size / 2 + extra_shift)

    avatar_url = None
    if user_json:
        avatar_url = user_json.get("avatar_url")

    avatar_img, avatar_drawn = download_and_draw_avatar(
        d,
        user_id=None,
        user_name=username,
        avatar_url=avatar_url,
        x=av_x,
        y=av_y,
        size=av_size,
        osu_api_client=osu_api_client,
    )

    if avatar_img and avatar_drawn:
        base.paste(avatar_img, (av_x, av_y), avatar_img)

    try:
        nb = d.textbbox((0, 0), username, font=SUBTITLE_FONT)
        n_w = nb[2] - nb[0]
        n_h = nb[3] - nb[1]
        name_x = av_x - 10 - n_w
        name_y = av_y + (av_size - n_h) // 2
        d.text((name_x, name_y), username, font=SUBTITLE_FONT, fill=username_color)
    except AttributeError:
        d.text(
            (av_x - 110, av_y + 25), username, font=SUBTITLE_FONT, fill=username_color
        )

    return title_right_x, title_h


def parse_sum(csv_path):
    summary = {}
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
        while lines and not lines[-1].strip():
            lines.pop()
        last_5 = lines[-5:]
        for line in last_5:
            parts = line.split(",", 1)
            if len(parts) == 2:
                k = parts[0].strip()
                v = parts[1].strip()
                summary[k] = v
    except Exception as e:
        logger.exception(f"Error parsing summary from CSV: {e}")
    return summary


def _prepare_image_data(user_id, user_name, mode, max_scores, osu_api_client=None):
    max_scores = max(1, min(100, max_scores))

    user_data_json = None
    if user_id:
        try:
            user_data_json = osu_api_client.user_osu(str(user_id), "id")
            if not user_data_json:
                logger.warning(
                    "User data not found for user_id %s (or user_name %s), image header might be incomplete",
                    user_id,
                    user_name,
                )
        except Exception:
            logger.exception(f"Error getting user data {user_id} for make_img:")

    if mode == "lost":
        csv_path = CSV_LOST
        out_path = IMG_LOST_OUT
        main_title = "Lost Scores"
        show_weights = False
        baseline_offset = 20
    else:
        csv_path = CSV_TOPLOST
        out_path = IMG_TOP_OUT
        main_title = "Potential Top"
        show_weights = True
        baseline_offset = 0

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            all_rows = list(csv.DictReader(f))
    except FileNotFoundError:
        logger.error(f"CSV file not found: {mask_path_for_log(csv_path)}")
        error_msg = (
            f"CSV file not found: {os.path.basename(mask_path_for_log(csv_path))}"
        )
        logger.error(
            "Error: %s, creating placeholder image for user %s", error_msg, user_name
        )
        create_placeholder_image(
            os.path.basename(out_path),
            user_name,
            error_msg,
        )
        return None
    except Exception as csv_err:
        logger.exception(f"Error reading CSV file {mask_path_for_log(csv_path)}:")
        error_msg = f"Error reading CSV file: {str(csv_err)}"
        logger.error(
            "Error: %s, creating placeholder image for user %s", error_msg, user_name
        )
        create_placeholder_image(
            os.path.basename(out_path),
            user_name,
            error_msg,
        )
        return None

    if not all_rows:
        logger.warning(
            f"No data in CSV file {mask_path_for_log(csv_path)} for image creation"
        )
        error_msg = f"No data to display in {mode} mode"
        logger.error(
            "Error: %s, creating placeholder image for user %s", error_msg, user_name
        )
        create_placeholder_image(os.path.basename(out_path), user_name, error_msg)
        return None

    total_rows_count = (
        max(0, len(all_rows) - 5) if show_weights else max(0, len(all_rows))
    )
    rows = all_rows[:max_scores]

    return {
        "user_data_json": user_data_json,
        "csv_path": csv_path,
        "out_path": out_path,
        "main_title": main_title,
        "show_weights": show_weights,
        "total_rows_count": total_rows_count,
        "rows": rows,
        "baseline_offset": baseline_offset,
        "mode": mode,
    }


def _process_user_statistics(user_data_json, show_weights, csv_path):
    cur_pp_val = 0
    cur_acc_f = 0.0
    if user_data_json and user_data_json.get("statistics"):
        raw_cur_pp = user_data_json["statistics"].get("pp", 0)
        cur_pp_val = round(float(raw_cur_pp))
        cur_acc_f = float(user_data_json["statistics"].get("hit_accuracy", 0.0))
    cur_acc_str = f"{cur_acc_f:.2f}%"

    stats = {
        "cur_pp_val": cur_pp_val,
        "cur_acc_str": cur_acc_str,
        "pot_pp_val": "N/A",
        "new_diff_pp": "N/A",
        "pot_acc_str": "N/A",
        "acc_diff_str": "N/A",
        "acc_diff_color": COLOR_WHITE,
        "diff_color": COLOR_WHITE,
    }

    if show_weights:
        top_summary = parse_sum(csv_path)

        raw_pot_pp_str = top_summary.get("Overall Potential PP", "0")
        try:
            pot_pp_val_num = round(float(raw_pot_pp_str))
            stats["pot_pp_val"] = str(pot_pp_val_num)
        except ValueError:
            pass

        diff_pp_str = top_summary.get("Difference", "0")
        try:
            diff_pp_f = float(diff_pp_str)
            sign_pp = "+" if diff_pp_f > 0 else ""
            stats["new_diff_pp"] = f"{sign_pp}{round(diff_pp_f)}"
            if diff_pp_f > 0:
                stats["diff_color"] = GREEN_COLOR
            elif diff_pp_f < 0:
                stats["diff_color"] = RED_COLOR
        except ValueError:
            pass

        pot_acc_str_raw = (
            top_summary.get("Overall Accuracy", "0%").replace("%", "").strip()
        )
        delta_acc_str_raw = (
            top_summary.get("Δ Overall Accuracy", "0%").replace("%", "").strip()
        )
        try:
            pot_acc_f = float(pot_acc_str_raw)
            stats["pot_acc_str"] = f"{pot_acc_f:.2f}%"

            acc_delta_f = float(delta_acc_str_raw)
            stats["acc_diff_str"] = f"{acc_delta_f:+.2f}%"
            if acc_delta_f > 0:
                stats["acc_diff_color"] = GREEN_COLOR
            elif acc_delta_f < 0:
                stats["acc_diff_color"] = RED_COLOR
        except ValueError:
            pass

    return stats


def _setup_canvas_and_dimensions(rows, mode, total_rows_count):
    threshold = MOD_THRESHOLD_LOST if mode == "lost" else MOD_THRESHOLD_TOP

    max_mods = 0
    for r in rows:
        mlist = short_mods(r.get("Mods", ""))
        if len(mlist) > max_mods:
            max_mods = len(mlist)

    extra_mods = max(0, max_mods - threshold)
    extra_width = extra_mods * MOD_EXTENSION_WIDTH
    card_w = DEFAULT_BASE_CARD_WIDTH + extra_width

    width = card_w + 2 * DEFAULT_MARGIN
    start_y = DEFAULT_MARGIN + TOP_PANEL_HEIGHT - (20 if mode == "lost" else 0)
    total_h = start_y + len(rows) * (CARD_HEIGHT + CARD_SPACING) + DEFAULT_MARGIN

    base = Image.new("RGBA", (width, total_h), COLOR_BG)
    d = ImageDraw.Draw(base)

    logger.info(f"Displaying {len(rows)}/{total_rows_count} scores in {mode} mode")

    return {"base": base, "d": d, "width": width, "card_w": card_w, "start_y": start_y}


def _draw_stats_section(d, stats, title_right_x, baseline_y):
    stats_start_x = title_right_x + 50
    stats_baseline = baseline_y + 5
    col_w = 140
    row1_y = stats_baseline
    row2_y = row1_y + 25

    def draw_col(label, val, x, y, val_color):
        try:
            label_box = d.textbbox((0, 0), label, font=VERSION_FONT)
            lw = label_box[2] - label_box[0]
            d.text((x, y), label, font=VERSION_FONT, fill=ACC_COLOR)
            d.text((x + lw + 5, y), str(val), font=VERSION_FONT, fill=val_color)
        except AttributeError:
            d.text((x, y), f"{label} {val}", font=VERSION_FONT, fill=val_color)

    draw_col("Cur PP:", stats["cur_pp_val"], stats_start_x, row1_y, COLOR_WHITE)
    draw_col(
        "Cur Acc:", stats["cur_acc_str"], stats_start_x + col_w, row1_y, COLOR_WHITE
    )
    draw_col(
        "Δ PP:",
        stats["new_diff_pp"],
        stats_start_x + 2 * col_w,
        row1_y,
        stats["diff_color"],
    )
    draw_col("Pot PP:", stats["pot_pp_val"], stats_start_x, row2_y, COLOR_WHITE)
    draw_col(
        "Pot Acc:", stats["pot_acc_str"], stats_start_x + col_w, row2_y, COLOR_WHITE
    )
    draw_col(
        "Δ Acc:",
        stats["acc_diff_str"],
        stats_start_x + 2 * col_w,
        row2_y,
        stats["acc_diff_color"],
    )


def make_img(
    user_id, user_name, mode="lost", max_scores=20, osu_api_client=None, gui_log=None
):
    logger.debug(
        "make_img called with: user_id=%s, user_name=%s, mode=%s, max_scores=%s",
        user_id,
        user_name,
        mode,
        max_scores,
    )

    if user_id is None or not user_name:
        max_scores = max(1, min(100, max_scores))
        logger.error("Invalid parameters: Need user_id and user_name")
        raise ValueError("Need user_id and user_name")

    if not osu_api_client:
        logger.error("Invalid parameters: API client not provided")
        raise ValueError("API client not provided")

    data = _prepare_image_data(user_id, user_name, mode, max_scores, osu_api_client)
    if data is None:
        logger.warning(
            "Image data preparation failed for user %s (%s), cannot generate image",
            user_name,
            user_id,
        )
        return

    stats = _process_user_statistics(
        data["user_data_json"], data["show_weights"], data["csv_path"]
    )

                                                      
    metadata_cache = {}
    if gui_log:
        gui_log("Pre-loading cover images...", update_last=True)
    preload_cover_images(data["rows"], metadata_cache, osu_api_client, gui_log)

    canvas_info = _setup_canvas_and_dimensions(
        data["rows"], data["mode"], data["total_rows_count"]
    )
    base = canvas_info["base"]
    d = canvas_info["d"]

    baseline_y = max(0, DEFAULT_MARGIN + 10 - data["baseline_offset"])
    extra_shift = 13 if data["mode"] == "lost" else 0
    av_size = 70

    title_right_x, title_h = draw_header(
        base,
        d,
        canvas_info["width"],
        DEFAULT_MARGIN,
        data["main_title"],
        user_name,
        USERNAME_COLOR,
        data["user_data_json"],
        av_size,
        baseline_y,
        title_h=40,
        extra_shift=extra_shift,
        osu_api_client=osu_api_client,
    )

    if data["show_weights"]:
        _draw_stats_section(d, stats, title_right_x, baseline_y)
    elif data["mode"] == "lost":
        scammed_y = baseline_y + title_h + 15
        s_ = f"Peppy scammed me for {data['total_rows_count']} of them!"
        d.text((DEFAULT_MARGIN, scammed_y), s_, font=VERSION_FONT, fill=COLOR_HIGHLIGHT)

    if gui_log:
        gui_log("Drawing score cards...", update_last=True)

    for i, row in enumerate(data["rows"]):
        card_x = DEFAULT_MARGIN
        card_y = canvas_info["start_y"] + i * (CARD_HEIGHT + CARD_SPACING)

        score_id_val = row.get("Score ID", "").strip().upper()
        is_lost_row = score_id_val == "LOST"

        draw_score_card(
            base,
            d,
            row,
            card_x,
            card_y,
            canvas_info["card_w"],
            CARD_HEIGHT,
            is_lost_row=is_lost_row,
            show_weights=data["show_weights"],
            metadata_cache=metadata_cache,
            osu_api_client=osu_api_client,
        )

    last_bottom = (
        canvas_info["start_y"]
        + len(data["rows"]) * (CARD_HEIGHT + CARD_SPACING)
        - CARD_SPACING
    )
    final_height = last_bottom + DEFAULT_MARGIN

    if final_height < base.height:
        base = base.crop((0, 0, canvas_info["width"], final_height))

    if gui_log:
        gui_log("Saving image...", update_last=True)

    base.save(data["out_path"])
    logger.info(
        "Image saved to %s", mask_path_for_log(os.path.normpath(data["out_path"]))
    )


def _adjust_max_scores_for_lost_score(max_scores, show_lost):
    if not show_lost:
        return max_scores

    top_with_lost_path = get_resource_path(os.path.join("csv", "top_with_lost.csv"))
    try:
        with open(top_with_lost_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        lost_score_rank = None
        for i, row in enumerate(rows, 1):
            if row.get("Score ID") == "LOST":
                lost_score_rank = i
                logger.info(f"Found first lost score at rank {lost_score_rank}")
                break

        if lost_score_rank is not None and lost_score_rank > max_scores:
            logger.info(
                f"Adjusting max_scores from {max_scores} to {lost_score_rank} to include lost score"
            )
            return lost_score_rank
        else:
            if lost_score_rank is None:
                logger.info("No lost scores found in the top")
            else:
                logger.info(
                    f"Lost score rank {lost_score_rank} is already within displayed top {max_scores}"
                )
            return max_scores
    except Exception as e:
        logger.error(f"Error finding lost score rank: {e}")
        return max_scores


def make_img_lost(
    user_id=None, user_name="", max_scores=20, osu_api_client=None, gui_log=None
):
    logger.debug(
        "make_img_lost called with: user_id=%s, user_name=%s, max_scores=%s",
        user_id,
        user_name,
        max_scores,
    )

    if not osu_api_client:
        logger.error("Invalid parameters: API client not provided")
        raise ValueError("API client not provided")

    make_img(
        user_id=user_id,
        user_name=user_name,
        mode="lost",
        max_scores=max_scores,
        osu_api_client=osu_api_client,
        gui_log=gui_log,
    )


def make_img_top(
    user_id=None,
    user_name="",
    max_scores=20,
    show_lost=False,
    osu_api_client=None,
    gui_log=None,
):
    logger.debug(
        "make_img_top called with: user_id=%s, user_name=%s, max_scores=%s, show_lost=%s",
        user_id,
        user_name,
        max_scores,
        show_lost,
    )

    if not osu_api_client:
        logger.error("Invalid parameters: API client not provided")
        raise ValueError("API client not provided")

    adjusted_max_scores = _adjust_max_scores_for_lost_score(max_scores, show_lost)

    make_img(
        user_id=user_id,
        user_name=user_name,
        mode="top",
        max_scores=adjusted_max_scores,
        osu_api_client=osu_api_client,
        gui_log=gui_log,
    )
