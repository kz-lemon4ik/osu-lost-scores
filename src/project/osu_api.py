import requests
import json
import re
import threading
import time
import logging
from config import CLIENT_ID, CLIENT_SECRET

logger = logging.getLogger(__name__)

api_lock = threading.Lock()
last_call = 0
session = requests.Session()

def wait_osu():
                                                  
    global last_call
    with api_lock:
        now = time.time()
        diff = now - last_call
        if diff<1:
            time.sleep(1 - diff)
        last_call = time.time()

def token_osu():
                                       
    wait_osu()
    url = "https://osu.ppy.sh/oauth/token"
    logger.info("POST: %s", url)
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "public"
    }
    resp = session.post(url, data=data)
    resp.raise_for_status()
    return resp.json().get("access_token")

def user_osu(profile_url, token):
                              
    wait_osu()
    match = re.search(r"osu\.ppy\.sh/users/(\d+)", profile_url)
    if match:
        uid = match.group(1)
    else:
        parts = profile_url.rstrip('/').split('/')
        uid = parts[-1]
    url = f"https://osu.ppy.sh/api/v2/users/{uid}"
    logger.info("GET user: %s", url)
    headers = {"Authorization": f"Bearer {token}"}
    resp = session.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def top_osu(token, user_id, limit=100):
                                               
    wait_osu()
    url = f"https://osu.ppy.sh/api/v2/users/{user_id}/scores/best?limit={limit}&include=beatmap"
    logger.info("GET top: %s", url)
    headers = {"Authorization": f"Bearer {token}"}
    resp = session.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def map_osu(beatmap_id, token):
                                        
    if not beatmap_id:
        return None
    wait_osu()
    url = f"https://osu.ppy.sh/api/v2/beatmaps/{beatmap_id}"
    logger.info("GET map: %s", url)
    headers = {"Authorization": f"Bearer {token}"}
    resp = session.get(url, headers=headers)
    if resp.status_code==404:
        return None
    resp.raise_for_status()
    data = resp.json()
    bset = data.get("beatmapset", {})

    c = data.get("count_circles", 0)
    s = data.get("count_sliders", 0)
    sp = data.get("count_spinners", 0)
    hobj = c + s + sp

    return {
        "status": data.get("status", "unknown"),
        "artist": bset.get("artist", ""),
        "title": bset.get("title", ""),
        "version": data.get("version", ""),
        "creator": bset.get("creator", ""),
        "hit_objects": hobj
    }
