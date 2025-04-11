import requests
import json
import re
import threading
import time
import logging
from config import CLIENT_ID, CLIENT_SECRET
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

api_lock = threading.Lock()
last_call = 0
session = requests.Session()

adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
session.mount("https://", adapter)
session.mount("http://", adapter)

TOKEN_CACHE = None

def wait_osu():
    global last_call
    with api_lock:
        now = time.time()
        diff = now - last_call
        if diff < 1/20:
            time.sleep((1/20) - diff)
        last_call = time.time()

def token_osu():
    global TOKEN_CACHE
    if TOKEN_CACHE is not None:
        return TOKEN_CACHE
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
    TOKEN_CACHE = resp.json().get("access_token")
    return TOKEN_CACHE

                                                              
def user_osu(identifier, lookup_key, token):
    wait_osu()
                       
    url = f"https://osu.ppy.sh/api/v2/users/{identifier}"
                               
    params = {
        'key': lookup_key                                              
    }
    logger.info("GET user: %s with params %s", url, params)
    headers = {"Authorization": f"Bearer {token}"}
                                 
    resp = session.get(url, headers=headers, params=params)                       
                                            
    if resp.status_code == 404:
         logger.error(f"Пользователь '{identifier}' (тип поиска: {lookup_key}) не найден.")
                                                                                   
                                                                      
         return None                          
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

def lookup_osu(checksum):
           
    wait_osu()
    url = "https://osu.ppy.sh/api/v2/beatmaps/lookup"
    headers = {
        "Authorization": f"Bearer {token_osu()}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    params = {"checksum": checksum}
    response = session.get(url, headers=headers, params=params)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
                                                                        
        if response.status_code == 404:
            logger.warning("Карта с checksum %s не найдена.", checksum)
            return None
        raise e
    data = response.json()
    return data.get("id")
