import os
import sys
import argparse
import requests
import json
import time
import re
from datetime import date
from threading import Lock
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load .env if present (for local runs)
load_dotenv()

# ── Config (Mapped to Environment Variables) ──────────────────────────────────
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
BASE_ID          = os.environ.get("AIRTABLE_BASE_ID")
TABLE_ID         = os.environ.get("AIRTABLE_TABLE_ID")
VIEW_ID          = os.environ.get("AIRTABLE_VIEW_ID")

# RapidAPI Keys (Comma separated in ENV)
env_keys = os.environ.get("RAPIDAPI_KEYS")
RAPIDAPI_KEYS = env_keys.split(",") if env_keys else []

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

key_lock = Lock()
key_index = 0

def get_next_rapidapi_key():
    global key_index
    with key_lock:
        key = RAPIDAPI_KEYS[key_index].strip()
        key_index = (key_index + 1) % len(RAPIDAPI_KEYS)
        return key

def update_running_stat(existing_json_str, new_value_str, date_str):
    """Maintain a JSON array of running statistics indicating daily changes."""
    try: new_val = int(new_value_str)
    except: return existing_json_str
        
    try: arr = json.loads(existing_json_str) if existing_json_str else []
    except: arr = []
        
    if arr and arr[-1].get("date") == date_str:
        last_entry = arr[-2] if len(arr) > 1 else None
        last_val = last_entry.get("count", new_val) if last_entry else new_val
        diff = new_val - last_val
        percent = (diff / last_val * 100) if last_val > 0 else 0.0
        arr[-1] = {"date": date_str, "count": new_val, "diff": diff, "percent": round(percent, 2)}
        return json.dumps(arr)
        
    last_val = arr[-1].get("count", new_val) if arr else new_val
    diff = new_val - last_val
    percent = (diff / last_val * 100) if last_val > 0 else 0.0
    arr.append({"date": date_str, "count": new_val, "diff": diff, "percent": round(percent, 2)})
    return json.dumps(arr)

# ── API Helpers ────────────────────────────────────────────────────────────────

def fetch_deezer_profile(deezer_id: str) -> dict:
    """Query Deezer RapidAPI for artist details."""
    if not deezer_id or deezer_id == "0":
        return None

    if not RAPIDAPI_KEYS:
        print("  [ERROR] No RAPIDAPI_KEYS found in environment!")
        return None

    key = get_next_rapidapi_key()
    url = f"https://deezerdevs-deezer.p.rapidapi.com/artist/{deezer_id}"
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": "deezerdevs-deezer.p.rapidapi.com"
    }

    try:
        # Debug: Show partial key and ID
        print(f"  --> Fetching ID: {deezer_id} (using Key ending in ...{key[-4:]})")
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 429:
            print(f"  [WARN] Rate limit hit on Key ending in ...{key[-4:]}. Throttling...")
            time.sleep(2)
        else:
            print(f"  [WARN] Deezer API returned {r.status_code} for ID {deezer_id}: {r.text}")
    except Exception as e:
        print(f"  [WARN] Deezer Request Failed: {e}")
        
    return None

def fetch_deezer_extended_data(deezer_id):
    """Fetch extended data using the public Deezer API with recursive fallback discovery."""
    base_url = f"https://api.deezer.com/artist/{deezer_id}"
    
    top_tracks = []
    try:
        r = requests.get(f"{base_url}/top?limit=10", timeout=2)
        if r.status_code == 200:
            for t in r.json().get('data', [])[:10]:
                top_tracks.append({
                    "name": t.get("title"),
                    "link": t.get("link"),
                    "album": t.get("album", {}).get("title"),
                    "image": t.get("album", {}).get("cover_xl") or t.get("album", {}).get("cover_medium"),
                    "duration": f"{int(t.get('duration', 0)) // 60}:{int(t.get('duration', 0)) % 60:02d}"
                })
    except Exception as e:
        print(f"    [WARN] Skipping Tracks due to timeout: {e}")

    related_artists = []
    try:
        r = requests.get(f"{base_url}/related?limit=10", timeout=2)
        if r.status_code == 200:
            for a in r.json().get('data', [])[:10]:
                related_artists.append({
                    "name": a.get("name"),
                    "link": a.get("link"),
                    "image": a.get("picture_xl") or a.get("picture_medium"),
                    "fans": a.get("nb_fan")
                })
    except Exception as e:
        print(f"    [WARN] Skipping Related due to timeout: {e}")

    related_playlists = []
    try:
        r = requests.get(f"{base_url}/playlists?limit=10", timeout=2)
        if r.status_code == 200:
            for p in r.json().get('data', [])[:10]:
                related_playlists.append({
                    "name": p.get("title"),
                    "link": p.get("link"),
                    "image": p.get("picture_xl") or p.get("picture_medium"),
                    "fans": p.get("fans", 0),
                    "tracks": p.get("nb_tracks", 0)
                })
    except Exception as e:
        print(f"    [WARN] Skipping Playlists due to timeout: {e}")

    def extract_app_state(html_text):
        marker = 'DZR_APP_STATE__ = '
        idx = html_text.find(marker)
        if idx == -1: return None
        json_start = idx + len(marker)
        try:
            decoder = json.JSONDecoder()
            obj, _ = decoder.raw_decode(html_text, json_start)
            return obj
        except: return None

    def find_bio_recursively(d_obj):
        best_str = ""
        def search(d):
            nonlocal best_str
            if isinstance(d, dict):
                for k, v in d.items():
                    if isinstance(v, str) and len(v) > len(best_str):
                        if '<script' not in v and '<style' not in v:
                            best_str = v
                    elif isinstance(v, (dict, list)):
                        search(v)
            elif isinstance(d, list):
                for item in d:
                    search(item)
        search(d_obj)
        return best_str

    def find_concerts_recursively(d_obj):
        best_list = []
        def search(d):
            nonlocal best_list
            if isinstance(d, list) and len(d) > 0:
                first = d[0]
                if isinstance(first, dict) and ('venue' in first or 'VENUE_NAME' in first) and ('date' in first or 'DATE' in first):
                    if len(d) > len(best_list):
                        best_list = d
            elif isinstance(d, dict):
                for v in d.values():
                    if isinstance(v, (dict, list)):
                        search(v)
        search(d_obj)
        return best_list

    bio = ""
    scrape_headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    try:
        r_bio = requests.get(f"https://www.deezer.com/en/artist/{deezer_id}/biography", headers=scrape_headers, timeout=4)
        if r_bio.status_code == 200:
            state = extract_app_state(r_bio.text)
            if state:
                raw_bio = find_bio_recursively(state)
                if raw_bio and len(raw_bio) > 100:
                    soup = BeautifulSoup(raw_bio, 'html.parser')
                    bio = soup.get_text('\n', strip=True)
    except Exception as e:
        print(f"    [WARN] Skipping Bio: {e}")

    concerts = []
    try:
        r_conc = requests.get(f"https://www.deezer.com/en/artist/{deezer_id}/concerts", headers=scrape_headers, timeout=4)
        if r_conc.status_code == 200:
            state = extract_app_state(r_conc.text)
            if state:
                concerts_raw = find_concerts_recursively(state)
                for c in concerts_raw:
                    concerts.append({
                        "id": c.get("id", c.get("CONCERT_ID")),
                        "name": c.get("name", c.get("NAME")),
                        "date": c.get("date", c.get("DATE")),
                        "venue": c.get("venue", c.get("VENUE_NAME")),
                        "city": c.get("city"),
                        "country": c.get("country")
                    })
    except Exception as e:
        print(f"    [WARN] Skipping Concerts: {e}")

    return {
        "Soc DZ Top Tracks JSON": json.dumps(top_tracks) if top_tracks else "",
        "Soc DZ Related Artists JSON": json.dumps(related_artists) if related_artists else "",
        "Soc DZ Related Playlists JSON": json.dumps(related_playlists) if related_playlists else "",
        "Soc DZ Bio": bio,
        "Soc DZ Concerts JSON": json.dumps(concerts) if concerts else ""
    }

def update_records_bulk(records_batch: list):
    """Update up to 10 records at once in Airtable."""
    if not records_batch:
        return True, {}
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    r = requests.patch(url, headers=AIRTABLE_HEADERS, json={"records": records_batch}, timeout=15)
    return r.status_code == 200, r.json()

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enrich Deezer profiles in Airtable.")
    parser.add_argument("--limit", type=int, default=None, help="Stop after processing N records")
    parser.add_argument("--all", action="store_true", help="Process all records in the view")
    args = parser.parse_args()

    if not args.all and args.limit is None:
        args.limit = 10 

    print(f"Starting Deezer enrichment from view {VIEW_ID}...")
    ok_count = 0
    err_count = 0
    processed_this_run = 0
    batch_queue = []
    
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    params = {
        "pageSize": 40,
        "view": VIEW_ID
    }
    today_str = date.today().isoformat()

    while True:
        try:
            r = requests.get(url, headers=AIRTABLE_HEADERS, params=params, timeout=15)
            data = r.json()
        except Exception as e:
            print(f"[RETRY] Connection error: {e}. Sleeping 5s...")
            time.sleep(5)
            continue

        if "error" in data:
            if data["error"].get("type") == "LIST_RECORDS_ITERATOR_NOT_AVAILABLE":
                print("[RECOVERY] Airtable cursor expired. Resetting pagination...")
                params.pop("offset", None)
                time.sleep(1)
                continue
            else:
                print(f"[ERROR] Airtable fetch: {data}")
                break
        
        page_records = data.get("records", [])
        if not page_records:
            break
            
        print(f"--- Processing page of {len(page_records)} records ---")
        for j, record in enumerate(page_records, 1):
            rec_id = record["id"]
            fields = record.get("fields", {})
            name   = fields.get("Name", "Unknown")
            dz_id  = fields.get("Soc DZ id", "").strip()

            processed_this_run += 1
            print(f"[{processed_this_run}] {name}")
            if not dz_id:
                print("  Skipping — No Deezer ID")
                continue

            api_data = fetch_deezer_profile(dz_id)
            if api_data and "error" not in api_data:
                update_data = {
                    "Soc DZ name": api_data.get("name", ""),
                    "Soc DZ link": api_data.get("link", ""),
                    "Soc DZ share": api_data.get("share", ""),
                    "Soc DZ picture_xl": api_data.get("picture_xl", ""),
                    "Soc DZ nb_album": str(api_data.get("nb_album", "")),
                    "Soc DZ nb_fan": str(api_data.get("nb_fan", "")),
                    "Soc DZ radio": "TRUE" if api_data.get("radio") else "FALSE",
                    "Soc DZ tracklist": api_data.get("tracklist", ""),
                    "Soc DZ type": api_data.get("type", "")
                }
                update_data.update(fetch_deezer_extended_data(dz_id))
                
                new_fans = api_data.get("nb_fan")
                if new_fans is not None:
                    update_data["Soc DZ nb_fan running"] = update_running_stat(
                        fields.get("Soc DZ nb_fan running", ""), str(new_fans), today_str
                    )
                update_data["Last Check"] = today_str
                update_data = {k: v for k, v in update_data.items() if v != ""}
                batch_queue.append({"id": rec_id, "fields": update_data})
            else:
                print(f"  API returned error or no data: {api_data}")
                err_count += 1

            if len(batch_queue) >= 10:
                print(f"  --> Sending bulk update for {len(batch_queue)} records...")
                success, resp = update_records_bulk(batch_queue)
                if success:
                    print("  ✅ Batch updated successfully")
                    ok_count += len(batch_queue)
                else:
                    print(f"  ❌ Batch update failed: {resp}")
                    err_count += len(batch_queue)
                batch_queue.clear()
                time.sleep(0.5)

            if args.limit and processed_this_run >= args.limit:
                break
            time.sleep(0.3)

        if batch_queue:
            print(f"  --> Final flush for page ({len(batch_queue)} records)...")
            success, resp = update_records_bulk(batch_queue)
            if success:
                print("  ✅ Batch updated successfully")
                ok_count += len(batch_queue)
            else:
                print(f"  ❌ Batch update failed: {resp}")
                err_count += len(batch_queue)
            batch_queue.clear()
            time.sleep(0.5)

        if args.limit and processed_this_run >= args.limit:
            break
        offset = data.get("offset")
        if not offset: break
        params["offset"] = offset

    print(f"\n{'='*50}\nDone. ✅ Enriched: {ok_count} | ❌ Failed/Skipped: {err_count}")

if __name__ == "__main__":
    main()
