import os
import csv
import requests
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import re
import json
import datetime
import logging
from database import db_get
from config import CLIENT_ID, CLIENT_SECRET
from utils import get_resource_path

logger = logging.getLogger(__name__)

BASE_DIR = get_resource_path("")
FONTS_DIR = get_resource_path(os.path.join("assets", "fonts"))
GRADES_DIR = get_resource_path(os.path.join("assets", "grades"))
MODS_DIR = get_resource_path(os.path.join("assets", "mod-icons"))

os.makedirs(os.path.join(BASE_DIR, "results"), exist_ok=True)

CSV_LOST = get_resource_path(os.path.join("csv", "lost_scores.csv"))
CSV_TOPLOST = get_resource_path(os.path.join("csv", "top_with_lost.csv"))
IMG_LOST_OUT = get_resource_path(os.path.join("results", "lost_scores_result.png"))
IMG_TOP_OUT = get_resource_path(os.path.join("results", "potential_top_result.png"))

AVATAR_DIR = get_resource_path(os.path.join("assets", "images", "avatar"))
COVER_DIR = get_resource_path(os.path.join("assets", "images", "cover"))
os.makedirs(AVATAR_DIR, exist_ok=True)
os.makedirs(COVER_DIR, exist_ok=True)

try:
    from PIL import ImageFont

    TITLE_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Bold.otf"), 36)
    SUBTITLE_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Regular.otf"), 18)
    MAP_NAME_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Italic.otf"), 18)
    CREATOR_SMALL_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Italic.otf"), 13)
    VERSION_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Italic.otf"), 14)
    SMALL_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-Regular.otf"), 16)
    BOLD_ITALIC_FONT = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-BoldItalic.otf"), 18)
    BOLD_ITALIC_FONT_SMALL = ImageFont.truetype(os.path.join(FONTS_DIR, "Exo2-BoldItalic.otf"), 14)
except Exception:
    print("Failed to load Exo2 fonts, using default fonts.")
    TITLE_FONT = SUBTITLE_FONT = MAP_NAME_FONT = CREATOR_SMALL_FONT = VERSION_FONT = SMALL_FONT = ImageFont.load_default()
    BOLD_ITALIC_FONT = BOLD_ITALIC_FONT_SMALL = ImageFont.load_default()

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


def create_placeholder_image(filename, username, message):
    width, height = 920, 400
    img = Image.new("RGBA", (width, height), COLOR_BG)
    draw = ImageDraw.Draw(img)

    draw.text((width // 2, 50), f"osu! Lost Scores Analyzer",
              font=TITLE_FONT, fill=ACC_COLOR, anchor="mm")

    draw.text((width // 2, 100), f"Player: {username}",
              font=SUBTITLE_FONT, fill=USERNAME_COLOR, anchor="mm")

    draw.text((width // 2, height // 2), message,
              font=SUBTITLE_FONT, fill=COLOR_WHITE, anchor="mm")

    draw.text((width // 2, height - 50), "Try running the analysis again or check for missing files",
              font=SMALL_FONT, fill=DATE_COLOR, anchor="mm")

    out_path = get_resource_path(os.path.join("results", filename))

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    img.save(out_path)
    print(f"Placeholder image saved to {out_path}")


def get_token_osu():
    url = "https://osu.ppy.sh/oauth/token"

    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")

    logger.info(f"Using keys: ID={client_id[:4]}...")

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "public"
    }

    try:
        r = requests.post(url, data=data)
        r.raise_for_status()
        token = r.json().get("access_token")
        if token:
            logger.info("API token successfully obtained for image generation")
            return token
        else:
            logger.error("Token not received in API response during image generation")
            return None
    except Exception as e:
        logger.error(f"Error getting token for image generation: {e}")
        return None


def get_user_osu(identifier, lookup_key, token):
    url = f"https://osu.ppy.sh/api/v2/users/{identifier}/osu"
    params = {
        'key': lookup_key
    }
    logger.info("GET user (image gen): %s with params %s", url, params)
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 404:
        logger.error(f"User '{identifier}' (lookup type: {lookup_key}) not found during image generation.")
        return None
    resp.raise_for_status()
    return resp.json()


def get_map_osu(bid, token):
    try:
        bid = int(bid)
    except:
        return None
    url = f"https://osu.ppy.sh/api/v2/beatmaps/{bid}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def dl_img(url, path):
    if os.path.exists(path):
        return

    os.makedirs(os.path.dirname(path), exist_ok=True)

    try:
        resp = requests.get(url)
        resp.raise_for_status()
        with open(path, "wb") as f:
            f.write(resp.content)
        return True
    except Exception as e:
        logger.warning(f"Failed to download image {url}: {e}")
        return False


def short_mods(mods_str):
    mlist = [m.strip() for m in mods_str.split(",") if m.strip()]
    return [m for m in mlist if m.upper() not in {"CL", "NM"}]


def short_txt(text, max_len=50):
    return text if len(text) <= max_len else text[:max_len - 3] + "..."


def since_date(date_str):
    try:
        dt = datetime.datetime.strptime(date_str, "%d-%m-%Y %H-%M-%S")
    except:
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


def parse_sum(csv_path):
    summary = {}
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
        while lines and not lines[-1].strip():
            lines.pop()
        last_5 = lines[-5:]
        for line in last_5:
            parts = line.split(',', 1)
            if len(parts) == 2:
                k = parts[0].strip()
                v = parts[1].strip()
                summary[k] = v
    except:
        pass
    return summary


def make_img(user_id, user_name, mode="lost", max_scores=20):
    token = get_token_osu()
    if user_id is None or not user_name:
        max_scores = max(1, min(100, max_scores))

        raise ValueError("Need user_id and user_name")

    max_scores = max(1, min(100, max_scores))

    user_data_json = None
    try:

        user_data_json = get_user_osu(str(user_id), 'id', token)
    except Exception as api_err:
        logger.error(f"Error getting user data {user_id} for make_img: {api_err}")

    if mode == "lost":
        csv_path = CSV_LOST
        out_path = IMG_LOST_OUT
        main_title = "Lost Scores"
        show_weights = False
    else:
        csv_path = CSV_TOPLOST
        out_path = IMG_TOP_OUT
        main_title = "Potential Top"
        show_weights = True

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            all_rows = list(csv.DictReader(f))
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_path}")
        print(f"Error: CSV file not found: {csv_path}")
        create_placeholder_image(os.path.basename(out_path), user_name,
                                 f"CSV file not found: {os.path.basename(csv_path)}")
        return
    except Exception as csv_err:
        logger.error(f"Error reading CSV file {csv_path}: {csv_err}")
        print(f"Error: Failed to read CSV file: {csv_path}")
        create_placeholder_image(os.path.basename(out_path), user_name, f"Error reading CSV file: {str(csv_err)}")
        return

    if not all_rows:
        logger.info(f"No data in CSV file {csv_path} for image creation")
        create_placeholder_image(os.path.basename(out_path), user_name, f"No data to display in {mode} mode")
        return

    total_rows_count = max(0, len(all_rows) - 5) if show_weights else max(0, len(all_rows))

    rows = all_rows[:max_scores]

    cur_pp_val = 0
    cur_acc_f = 0.0
    if user_data_json and user_data_json.get("statistics"):
        raw_cur_pp = user_data_json["statistics"].get("pp", 0)
        cur_pp_val = round(float(raw_cur_pp))
        cur_acc_f = float(user_data_json["statistics"].get("hit_accuracy", 0.0))
    cur_acc_str = f"{cur_acc_f:.2f}%"

    top_summary = {}
    pot_pp_val = "N/A"
    new_diff_pp = "N/A"
    pot_acc_str = "N/A"
    acc_diff_str = "N/A"
    acc_diff_color = COLOR_WHITE
    diff_color = COLOR_WHITE

    if show_weights:
        top_summary = parse_sum(csv_path)

        raw_pot_pp_str = top_summary.get("Overall Potential PP", "0")
        try:
            pot_pp_val_num = round(float(raw_pot_pp_str))
            pot_pp_val = str(pot_pp_val_num)
        except ValueError:
            pot_pp_val = "N/A"

        diff_pp_str = top_summary.get("Difference", "0")
        try:
            diff_pp_f = float(diff_pp_str)
            sign_pp = "+" if diff_pp_f > 0 else ""
            new_diff_pp = f"{sign_pp}{round(diff_pp_f)}"
            if diff_pp_f > 0:
                diff_color = GREEN_COLOR
            elif diff_pp_f < 0:
                diff_color = RED_COLOR
            else:
                diff_color = COLOR_WHITE
        except ValueError:
            new_diff_pp = "N/A"
            diff_color = COLOR_WHITE

        pot_acc_str_raw = top_summary.get("Overall Accuracy", "0%").replace('%', '').strip()
        delta_acc_str_raw = top_summary.get("Δ Overall Accuracy", "0%").replace('%', '').strip()
        try:
            pot_acc_f = float(pot_acc_str_raw)
            pot_acc_str = f"{pot_acc_f:.2f}%"

            acc_delta_f = float(delta_acc_str_raw)
            acc_diff_str = f"{acc_delta_f:+.2f}%"
            if acc_delta_f > 0:
                acc_diff_color = GREEN_COLOR
            elif acc_delta_f < 0:
                acc_diff_color = RED_COLOR
            else:
                acc_diff_color = COLOR_WHITE
        except ValueError:
            pot_acc_str = "N/A"
            acc_diff_str = "N/A"
            acc_diff_color = COLOR_WHITE

    logger.info(f"Displaying {len(rows)}/{total_rows_count} scores in {mode} mode")

    if mode == "lost":
        threshold = 4
    else:
        threshold = 2
    base_card_width = 920
    max_mods = 0
    for r in rows:
        mlist = short_mods(r.get("Mods", ""))
        if len(mlist) > max_mods: max_mods = len(mlist)
    extra_mods = max(0, max_mods - threshold)
    extra_width = extra_mods * 43
    card_w = base_card_width + extra_width

    MARGIN = 30
    card_h = 60
    spacing = 2
    top_panel_height = 80
    width = card_w + 2 * MARGIN
    if mode == "lost":
        baseline_offset = 20
    else:
        baseline_offset = 0
    start_y = MARGIN + top_panel_height - baseline_offset
    total_h = start_y + len(rows) * (card_h + spacing) + MARGIN

    base = Image.new("RGBA", (width, total_h), COLOR_BG)
    d = ImageDraw.Draw(base)

    baseline_y = max(0, MARGIN + 10 - baseline_offset)

    d.text((MARGIN, baseline_y), main_title, font=TITLE_FONT, fill=COLOR_WHITE)
    try:
        title_box = d.textbbox((MARGIN, baseline_y), main_title, font=TITLE_FONT)
        title_right_x = title_box[2]
        title_h = title_box[3] - title_box[1]
    except AttributeError:
        title_right_x = MARGIN + 200
        title_h = 40

    av_size = 70
    av_x = width - MARGIN - av_size
    center_y = baseline_y + title_h / 2
    extra_shift = 13 if mode == "lost" else 0
    av_y = int(center_y - av_size / 2 + extra_shift)

    avatar_filename = f"avatar_{user_name}.png"
    avatar_path = os.path.join(AVATAR_DIR, avatar_filename)
    avatar_url = None
    if user_data_json:
        avatar_url = user_data_json.get("avatar_url")

    avatar_drawn = False
    if avatar_url:
        try:
            dl_img(avatar_url, avatar_path)
            avatar_img_raw = Image.open(avatar_path).convert("RGBA").resize((av_size, av_size))
            av_mask = Image.new("L", (av_size, av_size), 0)
            ImageDraw.Draw(av_mask).rounded_rectangle((0, 0, av_size, av_size), radius=15, fill=255)
            avatar_img_raw.putalpha(av_mask)
            base.paste(avatar_img_raw, (av_x, av_y), avatar_img_raw)
            avatar_drawn = True
        except FileNotFoundError:
            logger.warning(f"Avatar file {avatar_path} not found after download attempt.")
        except Exception as img_err:
            logger.warning(f"Error processing avatar {avatar_path}: {img_err}.")

    if not avatar_drawn:
        logger.warning(f"Using placeholder for avatar user_id {user_id}.")
        d.rounded_rectangle((av_x, av_y, av_x + av_size, av_y + av_size), radius=15, fill=(80, 80, 80, 255))

    try:
        nb = d.textbbox((0, 0), user_name, font=SUBTITLE_FONT)
        n_w = nb[2] - nb[0]
        n_h = nb[3] - nb[1]
        name_x = av_x - 10 - n_w
        name_y = av_y + (av_size - n_h) // 2
        d.text((name_x, name_y), user_name, font=SUBTITLE_FONT, fill=USERNAME_COLOR)
    except AttributeError:
        d.text((av_x - 110, av_y + 25), user_name, font=SUBTITLE_FONT, fill=USERNAME_COLOR)

    if show_weights:
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

        draw_col("Cur PP:", cur_pp_val, stats_start_x, row1_y, COLOR_WHITE)
        draw_col("Cur Acc:", cur_acc_str, stats_start_x + col_w, row1_y, COLOR_WHITE)
        draw_col("Δ PP:", new_diff_pp, stats_start_x + 2 * col_w, row1_y, diff_color)
        draw_col("Pot PP:", pot_pp_val, stats_start_x, row2_y, COLOR_WHITE)
        draw_col("Pot Acc:", pot_acc_str, stats_start_x + col_w, row2_y, COLOR_WHITE)
        draw_col("Δ Acc:", acc_diff_str, stats_start_x + 2 * col_w, row2_y, acc_diff_color)
    elif mode == "lost":
        scammed_y = baseline_y + title_h + 15
        s_ = f"Peppy scammed me for {total_rows_count} of them!"
        d.text((MARGIN, scammed_y), s_, font=VERSION_FONT, fill=COLOR_HIGHLIGHT)

    for i, row in enumerate(rows):
        card_x = MARGIN
        card_y = start_y + i * (card_h + spacing)
        center_line = card_y + card_h // 2

        score_id_val = row.get("Score ID", "").strip().upper()
        is_lost_row = (score_id_val == "LOST")
        bg_color = COLOR_CARD_LOST if show_weights and is_lost_row else COLOR_CARD
        bg_img = Image.new("RGBA", (card_w, card_h), bg_color)

        beatmap_id = row.get("Beatmap ID", "").strip()
        raw_artist = ""
        raw_title = ""
        creator = ""
        version = ""
        cover_url = None

        beatmap_full_name = row.get("Beatmap", "")
        if beatmap_full_name:

            try:
                pattern = r"(.+) - (.+) \((.+)\) \[(.+)\]"
                match = re.match(pattern, beatmap_full_name)
                if match:
                    raw_artist, raw_title, creator, version = match.groups()
            except Exception as parse_err:
                logger.warning(f"Failed to parse beatmap name: {beatmap_full_name}")

        if not all([raw_artist, raw_title, creator, version]) and beatmap_id and beatmap_id.isdigit():
            try:
                db_info = db_get(beatmap_id)
                if db_info:
                    raw_artist = db_info.get("artist", raw_artist) or raw_artist
                    raw_title = db_info.get("title", raw_title) or raw_title
                    creator = db_info.get("creator", creator) or creator
                    version = db_info.get("version", version) or version
            except Exception as db_err:
                logger.warning(f"Error getting map data from DB {beatmap_id}: {db_err}")

        cover_file = None
        if beatmap_id and beatmap_id.isdigit():
            cover_file = os.path.join(COVER_DIR, f"cover_{beatmap_id}.png")
            if not os.path.exists(cover_file):

                try:

                    if (not all([raw_artist, raw_title, creator, version]) or not os.path.exists(
                            cover_file)) and beatmap_id:
                        bdata = get_map_osu(beatmap_id, token)
                        if bdata:

                            if "artist" in bdata:
                                raw_artist = bdata.get("artist") or raw_artist
                            if "title" in bdata:
                                raw_title = bdata.get("title") or raw_title
                            if "creator" in bdata:
                                creator = bdata.get("creator") or creator
                            if "version" in bdata:
                                version = bdata.get("version") or version

                            if bdata.get("beatmapset") and "covers" in bdata["beatmapset"]:
                                cover_url = bdata["beatmapset"]["covers"].get("cover@2x")
                except Exception as map_err:
                    logger.warning(f"Error getting map data {beatmap_id} from API: {map_err}")

        cover_w = card_w // 3
        cover_h_ = card_h
        c_img = None

        if cover_file and os.path.exists(cover_file):
            try:
                c_img = Image.open(cover_file).convert("RGBA").resize((cover_w, cover_h_))
            except Exception as cover_err:
                logger.warning(f"Failed to open cover {cover_file}: {cover_err}")
                c_img = None
        elif cover_url and beatmap_id:

            try:
                dl_img(cover_url, cover_file)
                if os.path.exists(cover_file):
                    c_img = Image.open(cover_file).convert("RGBA").resize((cover_w, cover_h_))
            except Exception as dl_err:
                logger.warning(f"Failed to download cover {cover_url}: {dl_err}")
                c_img = None

        if not c_img:
            c_img = Image.new("RGBA", (cover_w, cover_h_), (80, 80, 80, 255))

        fade_mask = Image.new("L", (cover_w, cover_h_), 255)
        dm_fade = ImageDraw.Draw(fade_mask)
        for x_ in range(cover_w):
            alpha_val = int(90 - (x_ / cover_w) * 90)
            dm_fade.line([(x_, 0), (x_, cover_h_)], fill=alpha_val)
        bg_img.paste(c_img, (0, 0), fade_mask)

        corner_mask = Image.new("L", (card_w, card_h), 0)
        dr_corner = ImageDraw.Draw(corner_mask)
        dr_corner.rounded_rectangle((0, 0, card_w, card_h), radius=15, fill=255)
        base.paste(bg_img, (card_x, card_y), corner_mask)

        d_card = ImageDraw.Draw(base)

        grade = row.get("Rank", "?")
        GRADE_TARGET_WIDTH = 45
        grade_icon_path = os.path.join(GRADES_DIR, f"{grade}.png")
        if os.path.isfile(grade_icon_path):
            try:
                g_img = Image.open(grade_icon_path).convert("RGBA")
                ow, oh = g_img.size
                scale = GRADE_TARGET_WIDTH / ow
                nw, nh = int(ow * scale), int(oh * scale)
                g_img_resized = g_img.resize((nw, nh), Image.Resampling.LANCZOS)
                base.paste(g_img_resized, (card_x + 10, center_line - nh // 2), g_img_resized)
            except Exception as grade_err:
                logger.warning(f"Error processing grade icon {grade_icon_path}: {grade_err}")
                d_card.text((card_x + 10, center_line - 8), grade, font=SUBTITLE_FONT, fill=COLOR_WHITE)
        else:
            d_card.text((card_x + 10, center_line - 8), grade, font=SUBTITLE_FONT, fill=COLOR_WHITE)

        full_name = short_txt(f"{raw_title} by {raw_artist}", 50)
        text_x = card_x + 70
        text_y_map = card_y + 4
        d_card.text((text_x, text_y_map), full_name, font=MAP_NAME_FONT, fill=COLOR_WHITE)
        text_y_map += 20
        d_card.text((text_x, text_y_map), f"by {creator}", font=CREATOR_SMALL_FONT, fill=COLOR_WHITE)
        text_y_map += 16
        date_str = row.get("Date", "")
        date_human = since_date(date_str)
        gap = "    "
        try:
            version_bbox = d_card.textbbox((0, 0), version, font=VERSION_FONT)
            version_w = version_bbox[2] - version_bbox[0]
            gap_bbox = d_card.textbbox((0, 0), gap, font=VERSION_FONT)
            gap_w = gap_bbox[2] - gap_bbox[0]
            d_card.text((text_x, text_y_map), version, font=VERSION_FONT, fill=COLOR_HIGHLIGHT)
            d_card.text((text_x + version_w + gap_w, text_y_map), date_human, font=VERSION_FONT, fill=DATE_COLOR)
        except AttributeError:
            d_card.text((text_x, text_y_map), f"{version}{gap}{date_human}", font=VERSION_FONT, fill=DATE_COLOR)

        shape_w = 100
        shape_left = card_x + card_w - shape_w
        right_block_x = shape_left - 20

        d_card.rounded_rectangle(
            (shape_left, card_y, shape_left + shape_w, card_y + card_h),
            radius=15,
            fill=PP_SHAPE_COLOR
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
            d_card.text((text_x_pp, text_y_pp), pp_str, font=SUBTITLE_FONT, fill=COLOR_WHITE)
        except AttributeError:
            d_card.text((shape_left + 15, center_line - 10), pp_str, font=SUBTITLE_FONT, fill=COLOR_WHITE)

        right_block_x = shape_left - 20

        if not show_weights:

            mods_edge = right_block_x - 90
            acc_center_x = (mods_edge + shape_left) / 2

            raw_acc_str = row.get("Accuracy", "0")
            try:
                acc_s = f"{float(raw_acc_str):.2f}%"
            except ValueError:
                acc_s = f"{raw_acc_str}%" if raw_acc_str else "?.??%"

            try:

                acc_box = d_card.textbbox((0, 0), acc_s, font=BOLD_ITALIC_FONT)
                d_card.text((acc_center_x, center_line), acc_s, font=BOLD_ITALIC_FONT,
                            fill=ACC_COLOR, anchor="mm")
            except AttributeError:

                acc_box = d_card.textbbox((0, 0), acc_s, font=BOLD_ITALIC_FONT)
                if acc_box:
                    acc_w = acc_box[2] - acc_box[0]
                    d_card.text((acc_center_x - acc_w / 2, center_line - 10), acc_s,
                                font=BOLD_ITALIC_FONT, fill=ACC_COLOR)
                else:
                    d_card.text((acc_center_x - 30, center_line - 10), acc_s,
                                font=BOLD_ITALIC_FONT, fill=ACC_COLOR)

            mods_right_edge = mods_edge
            mods_list = short_mods(row.get("Mods", ""))
            mod_x_cur = mods_right_edge
            for m_ in reversed(mods_list):
                path_ = os.path.join(MODS_DIR, f"{m_.upper()}.png")
                if os.path.isfile(path_):
                    try:
                        mg = Image.open(path_).convert("RGBA")
                        ow, oh = mg.size
                        sc = min(38 / ow, 38 / oh)
                        nw, nh = int(ow * sc), int(oh * sc)
                        mod_x_cur -= nw
                        mod_img_resized = mg.resize((nw, nh), Image.Resampling.LANCZOS)
                        base.paste(mod_img_resized, (int(mod_x_cur), center_line - nh // 2), mod_img_resized)
                        mod_x_cur -= 5
                    except Exception as mod_err:
                        logger.warning(f"Error processing mod icon {path_}: {mod_err}")
                else:
                    try:
                        box_m = d_card.textbbox((0, 0), m_, font=SMALL_FONT)
                        w_m = box_m[2] - box_m[0]
                        mod_x_cur -= w_m
                        d_card.text((mod_x_cur, center_line - 8), m_, font=SMALL_FONT, fill=COLOR_WHITE)
                        mod_x_cur -= 5
                    except AttributeError:
                        pass

        else:

            acc_column_width = 120
            pp_column_width = 70

            wpp_x = shape_left - 10
            raw_wpp = row.get("weight_PP", "")
            try:
                weight_pp_text = f"{round(float(raw_wpp))}pp"
            except ValueError:
                weight_pp_text = ""

            if weight_pp_text:

                try:

                    wpp_box = d_card.textbbox((0, 0), weight_pp_text, font=BOLD_ITALIC_FONT_SMALL)
                    wpp_w = wpp_box[2] - wpp_box[0]

                    d_card.text((wpp_x - pp_column_width / 2, center_line),
                                weight_pp_text,
                                font=BOLD_ITALIC_FONT_SMALL,
                                fill=WEIGHT_COLOR,
                                anchor="mm")
                except AttributeError:

                    d_card.text((wpp_x - pp_column_width + 5, center_line - 8),
                                weight_pp_text,
                                font=BOLD_ITALIC_FONT_SMALL,
                                fill=WEIGHT_COLOR)

            acc_block_x = wpp_x - pp_column_width - acc_column_width / 2

            raw_acc = row.get("Accuracy", "0")
            try:
                acc_str2 = f"{float(raw_acc):.2f}%"
            except ValueError:
                acc_str2 = f"{raw_acc}%" if raw_acc else "?.??%"

            raw_wpercent = row.get("weight_%", "")
            try:
                w_percent_str = f"weighted {round(float(raw_wpercent))}%"
            except ValueError:
                w_percent_str = ""

            try:

                acc_box = d_card.textbbox((0, 0), acc_str2, font=BOLD_ITALIC_FONT)
                acc_h = acc_box[3] - acc_box[1]

                left_align_x = acc_block_x - acc_column_width / 2 + 10

                vertical_spacing = 5

                d_card.text((left_align_x, center_line - acc_h / 2 - vertical_spacing),
                            acc_str2,
                            font=BOLD_ITALIC_FONT,
                            fill=ACC_COLOR,
                            anchor="lm")

                if w_percent_str:
                    wpct_box = d_card.textbbox((0, 0), w_percent_str, font=CREATOR_SMALL_FONT)
                    wpct_h = wpct_box[3] - wpct_box[1]

                    d_card.text((left_align_x, center_line + wpct_h / 2 + vertical_spacing),
                                w_percent_str,
                                font=CREATOR_SMALL_FONT,
                                fill=WEIGHT_COLOR,
                                anchor="lm")
            except AttributeError:

                d_card.text((acc_block_x - acc_column_width / 2 + 10, center_line - 14), acc_str2,
                            font=BOLD_ITALIC_FONT, fill=ACC_COLOR)
                if w_percent_str:
                    d_card.text((acc_block_x - acc_column_width / 2 + 10, center_line + 6), w_percent_str,
                                font=CREATOR_SMALL_FONT, fill=WEIGHT_COLOR)

            mods_right_edge = acc_block_x - acc_column_width / 2 - 10
            mods_list = short_mods(row.get("Mods", ""))
            mod_x_cur = mods_right_edge

            for m_ in reversed(mods_list):
                path_ = os.path.join(MODS_DIR, f"{m_.upper()}.png")
                if os.path.isfile(path_):
                    try:
                        mg = Image.open(path_).convert("RGBA")
                        ow, oh = mg.size
                        sc = min(38 / ow, 38 / oh)
                        nw, nh = int(ow * sc), int(oh * sc)
                        mod_x_cur -= nw
                        mod_img_resized = mg.resize((nw, nh), Image.Resampling.LANCZOS)
                        base.paste(mod_img_resized, (int(mod_x_cur), center_line - nh // 2), mod_img_resized)
                        mod_x_cur -= 5
                    except Exception as mod_err:
                        logger.warning(f"Error processing mod icon {path_}: {mod_err}")

                else:
                    try:
                        box_m = d_card.textbbox((0, 0), m_, font=SMALL_FONT)
                        w_m = box_m[2] - box_m[0]
                        mod_x_cur -= w_m
                        d_card.text((mod_x_cur, center_line - 8), m_, font=SMALL_FONT, fill=COLOR_WHITE)
                        mod_x_cur -= 5
                    except AttributeError:
                        pass

    last_bottom = start_y + len(rows) * (card_h + spacing) - spacing
    final_height = last_bottom + MARGIN

    if final_height < base.height:
        base = base.crop((0, 0, width, final_height))

    base.save(out_path)
    print(f"Image saved to {out_path}")


def make_img_lost(user_id=None, user_name="", max_scores=20):
    make_img(user_id=user_id, user_name=user_name, mode="lost", max_scores=max_scores)


def make_img_top(user_id=None, user_name="", max_scores=20):
    make_img(user_id=user_id, user_name=user_name, mode="top", max_scores=max_scores)
