import os
import csv
import logging
from database import db_get
from generate_image import (
    BASE_DIR,
    CSV_LOST,
    CSV_TOPLOST,
    get_token_osu,
    get_user_osu,
    get_map_osu,
    dl_img,
    grade_tot,
    short_mods as short_mods_orig,
    short_txt,
    since_date
)

logger = logging.getLogger(__name__)

HTML_LOST_OUT = os.path.join(os.path.dirname(__file__), "..", "results", "lost_scores_result.html")
HTML_TOP_OUT = os.path.join(os.path.dirname(__file__), "..", "results", "potential_top_result.html")

def short_mods(mods_str):
                                                  
    if not mods_str:
        return []
    try:
        return short_mods_orig(mods_str)
    except Exception as e:
        logger.warning("Mods err '%r': %s", mods_str, e)
        return []

def html_make(user_id, user_name, mode="lost"):
                                    
    logger.info("HTML: mode=%s, user_id=%s, user_name=%s", mode, user_id, user_name)
    token = get_token_osu()

    if mode == "lost":
        csv_path = CSV_LOST
        out_path = HTML_LOST_OUT
        title = "Lost Scores"
        show_weights = False
        base_offset = 20
    else:
        csv_path = CSV_TOPLOST
        out_path = HTML_TOP_OUT
        title = "Potential Top"
        show_weights = True
        base_offset = 0

    margin = 30
    cw = 940
    ch = 60
    spacing = 10
    top_height = 80

    width = cw + 2*margin
    start_y = margin + top_height - base_offset

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception as e:
        logger.error("CSV read error %s: %s", csv_path, e)
        return

    total_height = start_y + len(rows)*(ch+spacing) + margin
    if len(rows)==0:
        total_height = 300

    baseline = margin+10 - base_offset
    if baseline<0:
        baseline=0
    title_h = 36

    html = []
    html.append("<!DOCTYPE html>")
    html.append("<html lang='en'>")
    html.append("<head>")
    html.append("<meta charset='UTF-8'>")
    html.append(f"<title>{title}</title>")
    html.append('<link rel="stylesheet" href="style.css">')
    html.append("</head>")
    html.append("<body>")
    html.append(f'<div class="canvas" style="width:{width}px; min-height:{total_height}px;">')

    html.append(f'''
    <div class="title" style="left:{margin}px; top:{baseline}px;">
      {title}
    </div>
    ''')

    avatar_size = 70
    center = baseline+title_h/2
    extra = 13 if not show_weights else 0
    av_y = int(center - avatar_size/2 + extra)
    av_x = width - margin - avatar_size
    user_x = av_x - 120
    user_y = av_y + (avatar_size-18)//2

            
    html.append(f'''
    <div class="avatar" style="left:{av_x}px; top:{av_y}px;">
      <img src="../assets/images/avatar/avatar_{user_name}.png" alt="Avatar">
    </div>
    ''')

         
    html.append(f'''
    <div class="username" style="left:{user_x}px; top:{user_y}px;">
      {user_name}
    </div>
    ''')

    for i, row in enumerate(rows):
        card_x = margin
        card_y = start_y + i*(ch+spacing)

        beatmap_id = (row.get("Beatmap ID") or "").strip()
        if not beatmap_id.isdigit():
            logger.warning("Line #%d: bad ID '%r'. Skip.", i, beatmap_id)
            continue

        score_id_val = (row.get("Score ID") or "").strip().upper()
        card_class = "lost" if (show_weights and score_id_val=="LOST") else "normal"

        bdata = get_map_osu(beatmap_id, token)
        raw_artist = raw_title = creator = version = ""
        cover_url = None
        if bdata and bdata.get("beatmapset"):
            bs = bdata["beatmapset"]
            raw_artist = bs.get("artist","")
            raw_title = bs.get("title","")
            creator = bs.get("creator","")
            version = bdata.get("version","")
            cover_url = bs.get("covers",{}).get("cover@2x")

        cover_file = os.path.join(BASE_DIR, "assets", "images", "cover", f"cover_{beatmap_id}.png")
        if cover_url:
            try:
                dl_img(cover_url, cover_file)
            except Exception as e:
                logger.warning("Line #%d: fail cover %s", i, e)
        if not os.path.exists(cover_file):
            cover_rel = "../assets/images/cover/no_cover.png"
        else:
            cover_rel = f"../assets/images/cover/cover_{beatmap_id}.png"

        c100 = int(row.get("100") or 0)
        c50 = int(row.get("50") or 0)
        cMiss = int(row.get("Misses") or 0)
        grade = "?"
        db_ = db_get(beatmap_id)
        if db_ and db_.get("hit_objects",0)>0:
            tot = db_["hit_objects"]
            c300 = tot - (c100+c50+cMiss)
            if c300<0: c300=0
            grade = grade_tot(tot, c300, c50, cMiss)

        full_n = short_txt(f"{raw_title} by {raw_artist}", 50)
        date_str = row.get("Date","")
        date_h = since_date(date_str)
        try:
            pp_val = round(float(row.get("PP","0")))
        except:
            pp_val=0

        acc_r = row.get("Accuracy","0")
        try:
            f_acc = float(acc_r)
            acc_s = f"{f_acc:.2f}%"
        except:
            acc_s = "0.00%"

        wpp_r = row.get("weight_PP","0")
        wperc_r = row.get("weight_%","0")
        try:
            wpp_v = round(float(wpp_r))
        except:
            wpp_v=0
        try:
            wperc_v = int(round(float(wperc_r)))
        except:
            wperc_v=0

        mods_list = short_mods(row.get("Mods"))
        mods_html = ""
        for m in mods_list:
            mod_path = os.path.join(BASE_DIR, "assets", "mod-icons", f"{m.upper()}.png")
            if not os.path.exists(mod_path):
                mods_html += f'<span>{m}</span>'
            else:
                mods_html += f'<img src="../assets/mod-icons/{m.upper()}.png" alt="{m}">'

        if show_weights:
            block2 = f'''
              <div class="acc-line">{acc_s}</div>
              <div class="weight-line">weighted {wperc_v}%</div>
            '''
            block3 = f"{wpp_v}pp"
        else:
            block2 = f'<div class="acc-line">{acc_s}</div>'
            block3 = ""

        card_html = f'''
        <div class="card {card_class}" style="left:{card_x}px; top:{card_y}px;">
          <div class="cover" style="background-image: url('{cover_rel}');"></div>
          <div class="grade-icon">
            <img src="../assets/grades/{grade}.png" alt="{grade}">
          </div>
          <div class="map-info">
            <div class="title-line">{full_n}</div>
            <div class="creator-line">by {creator}</div>
            <div class="version-date-line">
              <span>{version}</span>
              <span class="date-ago">{date_h}</span>
            </div>
          </div>
          <div class="stats-row">
            <div class="mods-block">{mods_html}</div>
            <div class="acc-weight-block">{block2}</div>
            <div class="pp-weight-block">{block3}</div>
          </div>
          <div class="right" style="right:0; top:0;">
            <div class="pp">{pp_val}pp</div>
          </div>
        </div>
        '''
        html.append(card_html)

    html.append("</div>")
    html.append("</body>")
    html.append("</html>")

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(html))
        logger.info("HTML saved to %s", out_path)
        print(f"HTML сохранён в {out_path}")
    except Exception as e:
        logger.error("Fail save HTML: %s", e)

def html_lost(user_id, user_name):
                                    
    html_make(user_id, user_name, mode="lost")

def html_top(user_id, user_name):
                                 
    html_make(user_id, user_name, mode="top")
