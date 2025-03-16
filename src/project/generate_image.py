import os
import csv
import requests
from PIL import Image, ImageDraw, ImageFont
import re
import json
import datetime
import logging
from database import db_get
from config import CLIENT_ID, CLIENT_SECRET

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FONTS_DIR = os.path.join(BASE_DIR, "assets", "fonts")
GRADES_DIR = os.path.join(BASE_DIR, "assets", "grades")
MODS_DIR = os.path.join(BASE_DIR, "assets", "mod-icons")

os.makedirs(os.path.join(BASE_DIR, "results"), exist_ok=True)

         
CSV_LOST = os.path.join(BASE_DIR, "csv", "lost_scores.csv")
CSV_TOPLOST = os.path.join(BASE_DIR, "csv", "top_with_lost.csv")
IMG_LOST_OUT = os.path.join(BASE_DIR, "results", "lost_scores_result.png")
IMG_TOP_OUT = os.path.join(BASE_DIR, "results", "potential_top_result.png")

                 
AVATAR_DIR = os.path.join(BASE_DIR, "assets", "images", "avatar")
COVER_DIR = os.path.join(BASE_DIR, "assets", "images", "cover")
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
    print("Не удалось загрузить шрифты Exo2, используем стандартные.")
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

                    
                     
                    
def get_token_osu():
                                      
    url = "https://osu.ppy.sh/oauth/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "public"
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json().get("access_token")

def get_user_osu(profile_url, token):
                              
    match = re.search(r"osu\.ppy\.sh/users/(\d+)", profile_url)
    if match:
        uid = match.group(1)
    else:
        uid = profile_url.rstrip('/').split('/')[-1]
    url = f"https://osu.ppy.sh/api/v2/users/{uid}/osu"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
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
    resp = requests.get(url)
    resp.raise_for_status()
    with open(path, "wb") as f:
        f.write(resp.content)

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
    if ddays<0:
        return "Unknown date"
    months = ddays/30
    if months<1.5:
        return "about a month ago"
    if months<11.5:
        return f"{int(months+0.5)} months ago"
    yrs = months/12
    if yrs<1.5:
        return "a year ago"
    y_rounded = int(yrs+0.5)
    if y_rounded==1:
        return "a year ago"
    elif y_rounded==2:
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
            if len(parts)==2:
                k = parts[0].strip()
                v = parts[1].strip()
                summary[k] = v
    except:
        pass
    return summary

                              
                                  
                              
def make_img(user_id, user_name, mode="lost"):
    token = get_token_osu()
    if user_id is None or not user_name:
        raise ValueError("Need user_id and user_name")

    if mode=="lost":
        csv_path = CSV_LOST
        out_path = IMG_LOST_OUT
        main_title = "Lost Scores"
        show_weights = False
    else:
        csv_path = CSV_TOPLOST
        out_path = IMG_TOP_OUT
        main_title = "Potential Top"
        show_weights = True

            
    avatar_filename = f"avatar_{user_name}.png"
    avatar_path = os.path.join(AVATAR_DIR, avatar_filename)
    user_data_json = get_user_osu(f"https://osu.ppy.sh/users/{user_id}", token)
    if user_data_json.get("avatar_url"):
        dl_img(user_data_json["avatar_url"], avatar_path)
    avatar_img_raw = Image.open(avatar_path).convert("RGBA").resize((70,70))
    av_mask = Image.new("L", (70,70), 0)
    ImageDraw.Draw(av_mask).rounded_rectangle((0,0,70,70), radius=15, fill=255)
    avatar_img_raw.putalpha(av_mask)

                                
    with open(csv_path, "r", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))
    total_rows_count = len(all_rows)
    rows = all_rows[:20]

                             
    top_summary = {}
    if show_weights:
        top_summary = parse_sum(csv_path)

                         
    cur_pp_val = int(user_data_json.get("statistics",{}).get("pp",0))
    cur_acc_f = float(user_data_json.get("statistics",{}).get("hit_accuracy",0.0))
    cur_acc_str = f"{cur_acc_f:.2f}%"
    pot_pp_val = top_summary.get("Overall Potential PP","")
    pot_acc_val = top_summary.get("Overall Accuracy","")
    diff_pp_str = top_summary.get("Difference","")
    delta_acc_str = top_summary.get("Δ Overall Accuracy","")

                                                   
    if mode=="lost":
        threshold = 4
    else:
        threshold = 2
    base_card_width = 920
    max_mods = 0
    for r in rows:
        mlist = short_mods(r.get("Mods",""))
        if len(mlist)>max_mods:
            max_mods = len(mlist)
    extra_mods = max_mods - threshold
    if extra_mods<0:
        extra_mods=0
    extra_width = extra_mods * 43
    card_w = base_card_width + extra_width

             
    MARGIN = 30
    card_h = 60
    spacing = 10
    top_panel_height = 80
    width = card_w + 2*MARGIN
    if mode=="lost":
        baseline_offset=20
    else:
        baseline_offset=0

    start_y = MARGIN + top_panel_height - baseline_offset
    total_h = start_y + len(rows)*(card_h+spacing) + MARGIN

    base = Image.new("RGBA", (width, total_h), COLOR_BG)
    d = ImageDraw.Draw(base)

    baseline_y = MARGIN+10 - baseline_offset
    if baseline_y<0:
        baseline_y=0

               
    d.text((MARGIN, baseline_y), main_title, font=TITLE_FONT, fill=COLOR_WHITE)
    title_box = d.textbbox((MARGIN, baseline_y), main_title, font=TITLE_FONT)
    title_right_x = title_box[2]
    title_h = title_box[3]-title_box[1]

                         
    av_size=70
    av_x=width - MARGIN - av_size
    center_y = baseline_y + title_h/2
    extra_shift=13 if mode=="lost" else 0
    av_y=int(center_y - av_size/2 + extra_shift)
    base.paste(avatar_img_raw, (av_x, av_y), avatar_img_raw)
    nb = d.textbbox((0,0), user_name, font=SUBTITLE_FONT)
    n_w = nb[2]-nb[0]
    n_h = nb[3]-nb[1]
    name_x = av_x-10-n_w
    name_y = av_y + (av_size-n_h)//2
    d.text((name_x, name_y), user_name, font=SUBTITLE_FONT, fill=USERNAME_COLOR)

    if not show_weights:
        scammed_y = baseline_y + title_h + 15
        s_ = f"Peppy scammed me for {total_rows_count} of them!"
        d.text((MARGIN, scammed_y), s_, font=VERSION_FONT, fill=COLOR_HIGHLIGHT)
    else:
        stats_start_x = title_right_x + 50
        stats_baseline = baseline_y+5
        col_w=140
        row1_y=stats_baseline
        row2_y=row1_y+25

        def draw_col(label, val, x, y, val_color):
            label_box = d.textbbox((0,0), label, font=VERSION_FONT)
            lw = label_box[2]-label_box[0]
            d.text((x,y), label, font=VERSION_FONT, fill=ACC_COLOR)
            d.text((x+lw+5,y), val, font=VERSION_FONT, fill=val_color)

        diff_color = GREEN_COLOR
        draw_col("Cur PP:", str(cur_pp_val), stats_start_x, row1_y, COLOR_WHITE)
        draw_col("Cur Acc:", cur_acc_str, stats_start_x+col_w, row1_y, COLOR_WHITE)
        new_diff_pp = f"+{diff_pp_str}"
        draw_col("Δ PP:", new_diff_pp, stats_start_x+2*col_w, row1_y, diff_color)
        draw_col("Pot PP:", pot_pp_val, stats_start_x, row2_y, COLOR_WHITE)
        draw_col("Pot Acc:", pot_acc_val, stats_start_x+col_w, row2_y, COLOR_WHITE)
        draw_col("Δ Acc:", delta_acc_str, stats_start_x+2*col_w, row2_y, GREEN_COLOR)

    manual_offset_pp=-4

    for i, row in enumerate(rows):
        card_x=MARGIN
        card_y=start_y + i*(card_h+spacing)
        center_line=card_y + card_h//2

        score_id_val = row.get("Score ID","").strip().upper()
        is_lost_row = (score_id_val=="LOST")
        if show_weights and is_lost_row:
            bg_color=COLOR_CARD_LOST
        else:
            bg_color=COLOR_CARD

        bg_img=Image.new("RGBA", (card_w, card_h), bg_color)

        beatmap_id = row.get("Beatmap ID","").strip()
        c100 = int(row.get("100",0))
        c50 = int(row.get("50",0))
        cMiss = int(row.get("Misses",0))

        bdata=None
        if beatmap_id.isdigit():
            try:
                bdata = get_map_osu(beatmap_id, token)
            except:
                bdata=None

        raw_artist=raw_title=creator=version=""
        if bdata and bdata.get("beatmapset"):
            cover2x = bdata["beatmapset"]["covers"].get("cover@2x")
            raw_artist=bdata["beatmapset"].get("artist","")
            raw_title=bdata["beatmapset"].get("title","")
            creator=bdata["beatmapset"].get("creator","")
            version=bdata.get("version","")
        else:
            cover2x=None

        cover_w = card_w//3
        cover_h_ = card_h
        if cover2x:
            cover_file = os.path.join(COVER_DIR, f"cover_{beatmap_id}.png")
            try:
                dl_img(cover2x, cover_file)
                c_img = Image.open(cover_file).convert("RGBA").resize((cover_w, cover_h_))
            except:
                c_img=None
        else:
            c_img=None
        if not c_img:
            c_img=Image.new("RGBA", (cover_w, cover_h_), (80,80,80,255))

        fade_mask=Image.new("L",(cover_w,cover_h_),255)
        dm=ImageDraw.Draw(fade_mask)
        for x_ in range(cover_w):
            alpha_val=int(90 - (x_/cover_w)*90)
            dm.line([(x_,0),(x_,cover_h_)], fill=alpha_val)
        bg_img.paste(c_img,(0,0), fade_mask)

        corner_mask=Image.new("L",(card_w,card_h),0)
        dr_=ImageDraw.Draw(corner_mask)
        dr_.rounded_rectangle((0,0,card_w,card_h), radius=15, fill=255)
        base.paste(bg_img,(card_x,card_y), corner_mask)

        grade = row.get("Rank", "?")

        d_=ImageDraw.Draw(base)
        GRADE_TARGET_WIDTH=45
        grade_icon=os.path.join(GRADES_DIR,f"{grade}.png")
        if os.path.isfile(grade_icon):
            g_img=Image.open(grade_icon).convert("RGBA")
            ow, oh=g_img.size
            scale=GRADE_TARGET_WIDTH/ow
            nw, nh=int(ow*scale), int(oh*scale)
            base.paste(g_img.resize((nw,nh), Image.LANCZOS),
                       (card_x+10, center_line-nh//2),
                       g_img.resize((nw,nh), Image.LANCZOS))
        else:
            d_.text((card_x+10, center_line-8), grade, font=SUBTITLE_FONT, fill=COLOR_WHITE)

        full_name=short_txt(f"{raw_title} by {raw_artist}",50)
        text_x=card_x+70
        text_y=card_y+4
        d_.text((text_x, text_y), full_name, font=MAP_NAME_FONT, fill=COLOR_WHITE)
        text_y+=20
        d_.text((text_x, text_y), f"by {creator}", font=CREATOR_SMALL_FONT, fill=COLOR_WHITE)
        text_y+=16
        date_str=row.get("Date","")
        date_human=since_date(date_str)
        gap="    "
        version_bbox=d_.textbbox((0,0), version, font=VERSION_FONT)
        gap_bbox=d_.textbbox((0,0), gap, font=VERSION_FONT)
        d_.text((text_x, text_y), version, font=VERSION_FONT, fill=COLOR_HIGHLIGHT)
        d_.text((text_x + (version_bbox[2]-version_bbox[0]) + (gap_bbox[2]-gap_bbox[0]), text_y),
                date_human, font=VERSION_FONT, fill=DATE_COLOR)

                               
        shape_w=100
        shape_left=card_x+card_w-shape_w
        pp_bg=Image.new("RGBA",(shape_w,card_h), PP_SHAPE_COLOR)
        mask_pp=Image.new("L",(shape_w,card_h),0)
        dr_pp=ImageDraw.Draw(mask_pp)
        dr_pp.rounded_rectangle((0,0,shape_w,card_h), radius=15, fill=255)
        pp_bg.putalpha(mask_pp)
        base.paste(pp_bg,(shape_left,card_y), pp_bg)

        raw_pp=row.get("PP","0")
        try:
            pp_val=round(float(raw_pp))
        except:
            pp_val=0
        pp_str=f"{pp_val}pp"
        box_pp=d_.textbbox((0,0),pp_str,font=SUBTITLE_FONT)
        w_pp_=box_pp[2]-box_pp[0]
        h_pp_=box_pp[3]-box_pp[1]
        manual_offset_pp=-4
        text_x_pp=shape_left + shape_w/2 - w_pp_/2
        text_y_pp=center_line - h_pp_/2+manual_offset_pp
        d_.text((text_x_pp,text_y_pp), pp_str, font=SUBTITLE_FONT, fill=COLOR_WHITE)

        if not show_weights:
            slot_gap=20
            ACCURACY_SLOT_WIDTH=80
            slot_right=shape_left-slot_gap
            slot_left=slot_right-ACCURACY_SLOT_WIDTH
            raw_acc_str=row.get("Accuracy","0")
            try:
                acc_f=float(raw_acc_str)
                acc_s=f"{acc_f:.2f}%"
            except:
                acc_s=f"{raw_acc_str}%"
            d_.text((slot_left,center_line-10),
                    acc_s, font=BOLD_ITALIC_FONT, fill=ACC_COLOR)
            mods_right=slot_left-slot_gap-10
            mods_list=short_mods(row.get("Mods",""))
            x_cur=mods_right
            for m_ in reversed(mods_list):
                path_=os.path.join(MODS_DIR,f"{m_.upper()}.png")
                if os.path.isfile(path_):
                    mg=Image.open(path_).convert("RGBA")
                    ow, oh=mg.size
                    sc=min(38/ow, 38/oh)
                    nw, nh=int(ow*sc), int(oh*sc)
                    x_cur-=nw
                    base.paste(mg.resize((nw,nh), Image.LANCZOS),
                               (int(x_cur),center_line-nh//2),
                               mg.resize((nw,nh), Image.LANCZOS))
                    x_cur-=5
                else:
                    box_m=d_.textbbox((0,0),m_,font=SMALL_FONT)
                    w_m=box_m[2]-box_m[0]
                    x_cur-=w_m
                    d_.text((x_cur, center_line-8), m_, font=SMALL_FONT, fill=COLOR_WHITE)
                    x_cur-=5
        else:
            BLOCK_SPACING=20
            WPP_SLOT_WIDTH=70
            wpp_slot_right=shape_left - BLOCK_SPACING
            wpp_slot_left=wpp_slot_right - WPP_SLOT_WIDTH
            raw_wpp=row.get("weight_PP","")
            if raw_wpp:
                try:
                    wpp_val2=round(float(raw_wpp))
                except:
                    wpp_val2=0
                weight_pp_text=f"{wpp_val2}pp"
            else:
                weight_pp_text=""
            if weight_pp_text:
                d_.text((wpp_slot_left, center_line-8),
                        weight_pp_text, font=BOLD_ITALIC_FONT_SMALL, fill=WEIGHT_COLOR)

            ACC_SLOT_WIDTH=80
            acc_slot_right=wpp_slot_left - BLOCK_SPACING
            acc_slot_left=acc_slot_right - ACC_SLOT_WIDTH
            raw_acc=row.get("Accuracy","0")
            try:
                acc_val=float(raw_acc)
                acc_str2=f"{acc_val:.2f}%"
            except:
                acc_str2=f"{raw_acc}%"
            raw_wpercent=row.get("weight_%","")
            if raw_wpercent:
                try:
                    w_p=round(float(raw_wpercent))
                except:
                    w_p=0
                w_percent_str=f"weighted {w_p}%"
            else:
                w_percent_str=""
            d_.text((acc_slot_left, center_line-20),
                    acc_str2, font=BOLD_ITALIC_FONT, fill=ACC_COLOR)
            if w_percent_str:
                d_.text((acc_slot_left, center_line+5),
                        w_percent_str, font=CREATOR_SMALL_FONT, fill=WEIGHT_COLOR)

            mods_right=acc_slot_left - BLOCK_SPACING - 10
            mods_list=short_mods(row.get("Mods",""))
            x_cur=mods_right
            for m_ in reversed(mods_list):
                path_=os.path.join(MODS_DIR, f"{m_.upper()}.png")
                if os.path.isfile(path_):
                    mg=Image.open(path_).convert("RGBA")
                    ow, oh=mg.size
                    sc=min(38/ow, 38/oh)
                    nw, nh=int(ow*sc), int(oh*sc)
                    x_cur-=nw
                    base.paste(mg.resize((nw,nh), Image.LANCZOS),
                               (int(x_cur), center_line-nh//2),
                               mg.resize((nw,nh), Image.LANCZOS))
                    x_cur-=5
                else:
                    box_m=d_.textbbox((0,0), m_, font=SMALL_FONT)
                    w_m=box_m[2]-box_m[0]
                    x_cur-=w_m
                    d_.text((x_cur, center_line-8), m_, font=SMALL_FONT, fill=COLOR_WHITE)
                    x_cur-=5

    last_bottom = start_y + (len(rows)-1)*(card_h+spacing) + card_h
    final_height = last_bottom+MARGIN
    if final_height<base.height:
        base = base.crop((0,0,width, final_height))

    base.save(out_path)
    print(f"Изображение сохранено в {out_path}")

                        
                          
                        
def make_img_lost(csv_path=CSV_LOST, out_path=IMG_LOST_OUT, user_id=None, user_name=""):
    make_img(user_id=user_id, user_name=user_name, mode="lost")

def make_img_top(csv_path=CSV_TOPLOST, out_path=IMG_TOP_OUT, user_id=None, user_name=""):
    make_img(user_id=user_id, user_name=user_name, mode="top")
