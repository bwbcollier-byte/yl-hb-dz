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

import unicodedata

def normalize_name(name: str) -> str:
    """Remove accents and special characters for looser matching."""
    if not name: return ""
    # Normalize unicode (accents) and convert to ASCII
    nfkd_form = unicodedata.normalize('NFKD', name)
    only_ascii = nfkd_form.encode('ASCII', 'ignore').decode('ASCII')
    # Remove common punctuation and double spaces
    clean = "".join(c for c in only_ascii if c.isalnum() or c.isspace()).lower()
    return " ".join(clean.split())

def search_deezer_artist(name: str) -> str:
    """Search Deezer for the correct artist ID by name."""
    key = get_next_rapidapi_key()
    url = "https://deezerdevs-deezer.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": "deezerdevs-deezer.p.rapidapi.com"
    }
    target_norm = normalize_name(name)
    try:
        r = requests.get(url, headers=headers, params={"q": name}, timeout=10)
        if r.status_code == 200:
            data = r.json().get("data", [])
            for item in data:
                artist = item.get("artist", {})
                found_name = artist.get("name", "")
                if normalize_name(found_name) == target_norm:
                    return str(artist.get("id"))
    except:
        pass
    return None

def fetch_deezer_profile(deezer_id: str, artist_name: str = None) -> tuple:
    """Query Deezer RapidAPI for artist details. Returns (data, corrected_id)."""
    if not deezer_id or deezer_id == "0":
        return None, None

    if not RAPIDAPI_KEYS:
        print("  [ERROR] No RAPIDAPI_KEYS found in environment!")
        return None, None

    key = get_next_rapidapi_key()
    url = f"https://deezerdevs-deezer.p.rapidapi.com/artist/{deezer_id}"
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": "deezerdevs-deezer.p.rapidapi.com"
    }

    try:
        print(f"  --> Fetching ID: {deezer_id} (using Key ending in ...{key[-4:]})")
        r = requests.get(url, headers=headers, timeout=10)
        
        # If ID is wrong (code 800), try to find the correct one via Search
        if r.status_code == 200:
            resp_data = r.json()
            if "error" in resp_data and resp_data["error"].get("code") == 800 and artist_name:
                print(f"    [FIX] ID {deezer_id} invalid. Searching for '{artist_name}'...")
                new_id = search_deezer_artist(artist_name)
                if new_id:
                    print(f"    [FOUND] New ID: {new_id}. Retrying...")
                    # Recursive call with the NEW ID
                    data, _ = fetch_deezer_profile(new_id, None)
                    return data, new_id
                else:
                    print(f"    [FAILED] Could not find a match for '{artist_name}'")
            return resp_data, None
        elif r.status_code == 429:
            print(f"  [WARN] Rate limit hit. Throttling...")
            time.sleep(2)
        else:
            print(f"  [WARN] Deezer API returned {r.status_code}: {r.text}")
    except Exception as e:
        print(f"  [WARN] Deezer Request Failed: {e}")
        
    return None, None

def fetch_deezer_extended_data(deezer_id):
    """Fetch extended data using the public Deezer API with recursive fallback discovery."""
    base_url = f"https://api.deezer.com/artist/{deezer_id}"
    
    top_tracks = []
    try:
        r = requests.get(f"{base_url}/top?limit=10", timeout=3)
        if r.status_code == 200:
            for t in r.json().get('data', [])[:10]:
                top_tracks.append(t.get("title"))
    except Exception as e:
        print(f"    [WARN] Skipping Tracks due to timeout: {e}")

    related_names = []
    related_urls = []
    try:
        r = requests.get(f"{base_url}/related?limit=10", timeout=3)
        if r.status_code == 200:
            for a in r.json().get('data', [])[:10]:
                related_names.append(a.get("name"))
                related_urls.append(a.get("link"))
    except Exception as e:
        print(f"    [WARN] Skipping Related due to timeout: {e}")

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

    bio = ""
    scrape_headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    try:
        r_bio = requests.get(f"https://www.deezer.com/en/artist/{deezer_id}/biography", headers=scrape_headers, timeout=5)
        if r_bio.status_code == 200:
            state = extract_app_state(r_bio.text)
            if state:
                raw_bio = find_bio_recursively(state)
                if raw_bio and len(raw_bio) > 100:
                    soup = BeautifulSoup(raw_bio, 'html.parser')
                    bio = soup.get_text('\n', strip=True)
    except Exception as e:
        print(f"    [WARN] Skipping Bio: {e}")

    return {
        "dez_top_media": ", ".join(top_tracks) if top_tracks else "",
        "dez_related_name": ", ".join(related_names) if related_names else "",
        "dez_related_urls": ", ".join(related_urls) if related_urls else "",
        "dez_description": bio
    }

def update_records_bulk(records_batch: list):
    """Update up to 10 records at once in Airtable."""
    if not records_batch:
        return True, {}
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    r = requests.patch(url, headers=AIRTABLE_HEADERS, json={"records": records_batch}, timeout=15)
    return r.status_code == 200, r.json()

def extract_id_from_url(url: str) -> str:
    """Extract numeric artist ID from Deezer URL."""
    if not url: return None
    match = re.search(r'artist/(\d+)', url)
    return match.group(1) if match else None

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
            
            # Read from new URL column if ID is missing
            dz_id  = fields.get("dez_identifier", "").strip()
            dz_url = fields.get("soc_deezer", "").strip()
            
            if not dz_id and dz_url:
                dz_id = extract_id_from_url(dz_url)

            processed_this_run += 1
            print(f"[{processed_this_run}] {name}")
            if not dz_id:
                print("  Skipping — No Deezer ID or URL")
                continue

            api_data, corrected_id = fetch_deezer_profile(dz_id, name)

            if api_data and "error" not in api_data:
                update_data = {
                    "dez_name": api_data.get("name", ""),
                    "dez_url": api_data.get("link", ""),
                    "dez_share": api_data.get("share", ""),
                    "dez_image": api_data.get("picture_xl", ""),
                    "dez_media_count": str(api_data.get("nb_album", "")),
                    "dez_followers": str(api_data.get("nb_fan", "")),
                    "dez_verified": "TRUE" if api_data.get("radio") else "FALSE",
                    "dez_last_check": today_str
                }
                
                # If we corrected the ID during discovery, save the NEW ONE to Airtable
                if corrected_id:
                    update_data["dez_identifier"] = corrected_id
                    dz_id = corrected_id 
                    print(f"    [SAVING] Will update Airtable with correct ID: {corrected_id}")
                else:
                    update_data["dez_identifier"] = dz_id

                # --- EXTENDED DATA ---
                update_data.update(fetch_deezer_extended_data(dz_id))
                
                new_fans = api_data.get("nb_fan")
                if new_fans is not None:
                    update_data["dez_running_follower"] = update_running_stat(
                        fields.get("dez_running_follower", ""), str(new_fans), today_str
                    )
                
                # Clean up empty values
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

    print(f"\n{'='*50}\nDone. ✅ Enriched: {ok_count} | {err_count} Failed/Skipped")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
