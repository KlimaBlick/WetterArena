#!/usr/bin/env python3
"""
build.py – WetterArena v8.2.3
• GeoSphere-Tagesdaten → Supabase-Postgres → site/last7.csv (+index.html)
• Neu v8.2.3
  – fängt alle requests.exceptions.RequestException (z. B. Connection reset) ab
  – Retry-Logik wie bei 5xx, trotzdem hartes Limit (MAX_RETRIES)
  – Beispielwerte: PAUSE_EVERY = 10, PAUSE_SECONDS = 5 → ≈ 120 req/h
"""

from __future__ import annotations
import argparse, csv, datetime as dt, os, sys, textwrap, time
from itertools import islice
from typing import Iterable, List

import psycopg2, requests
from requests.exceptions import RequestException

# ─── Konfiguration ────────────────────────────────────────────

PG_URI = os.environ["PG_URI"]       # postgresql://postgres:<PW>@db.xxx.supabase.co:5432/postgres
META_CSV = "stations.csv"
DATASET = "klima-v2-1d"
BASE = "https://dataset.api.hub.geosphere.at/v1"

SITE_DIR = "site"
TEMPLATE = "index_template.html"

#   API & Throttling
SESSION = requests.Session()
TIMEOUT = 90                  # Sekunden pro HTTP-Call
MAX_RETRIES = 3               # 1 Original + 3 Wiederholungen
BACKOFF0 = 30                 # erster 429-Sleep
MAX_BACKOFF = 300             # nie länger als 5 min warten
PAUSE_EVERY = 10              # proaktive Pause nach jeder 10. Request
PAUSE_SECONDS = 5             # …für 5 s

CHUNK_SIZE = 50               # Stationen pro API-Aufruf

PARAMS_DEFAULT = [
  "bewd_i","bewd_ii","bewd_iii","bewm_i","bewm_ii","bewm_iii","bewm_mittel",
  "bft6","bft8","cglo_j","dampf_i","dampf_ii","dampf_iii","dampf_mittel",
  "dd32_i","dd32_ii","dd32_iii","erdb_i","erdb_ii","erdb_iii","ffx","gew",
  "glatt","nebel","p_i","p_ii","p_iii","p_mittel","raureif","reif",
  "rfb_i","rfb_ii","rfb_iii","rfb_mittel","rf_i","rf_ii","rf_iii","rf_mittel",
  "rr","rr_i","rr_iii","rra_manu","rra_manu_i","rra_manu_iii",
  "sh","sh_manu","sha_manu","shneu_manu","sicht_i","sicht_ii","sicht_iii",
  "so_h","tau","tl_i","tl_ii","tl_iii","tlmax","tlmin","tl_mittel","tsmin",
  "vvbft_i","vvbft_ii","vvbft_iii","vv_mittel","zeitx"
]
META_COLS = ["station","name","state","lat","lon","date"]

# ─── Helpers ──────────────────────────────────────────────────

def grouper(it: Iterable[int], n: int) -> Iterable[List[int]]:
    it = iter(it)
    while (chunk := list(islice(it, n))):
        yield chunk

def log(*a, **kw): print(*a, file=sys.stderr, **kw)

def load_stations() -> tuple[list[int], dict[int,dict]]:
    stations, meta = [], {}
    today = dt.date.today()
    with open(META_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                valid_to = dt.date.fromisoformat(row.get("Enddatum","")[:10])
            except ValueError:
                continue
            if valid_to < today:               # nur aktive
                continue
            sid = int(row["id"])
            stations.append(sid)
            meta[sid] = {
                "name":  row.get("Stationsname","").strip(),
                "state": row.get("Bundesland","").strip()
            }
    return stations, meta

STATIONS_ALL, STATION_META = load_stations()
REQ_COUNT = 0                                    # globaler Counter

# ─── Netzwerk / API ───────────────────────────────────────────

def fetch_json(day: dt.date, ids: list[int], params: list[str]) -> dict|None:
    global REQ_COUNT
    url = (
        f"{BASE}/station/historical/{DATASET}"
        f"?start={day}&end={day}"
        f"&station_ids={','.join(map(str, ids))}"
        f"&parameters={','.join(p.upper() for p in params)}"
    )

    retries, backoff = 0, BACKOFF0
    while True:
        # proaktive Bremse
        if REQ_COUNT and REQ_COUNT % PAUSE_EVERY == 0:
            log(f"⏸  proactive pause {PAUSE_SECONDS}s …")
            time.sleep(PAUSE_SECONDS)

        REQ_COUNT += 1
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
        except RequestException as e:                       # Netz-Fehler
            log(f"⚠️  network error ({e.__class__.__name__}) – try {retries+1}/{MAX_RETRIES}")
            r = None

        # ---------- Erfolgs-Pfad ----------
        if r and r.status_code == 200:
            return r.json()

        # ---------- Abbruch ohne Retry ----------
        if r and r.status_code in (403, 404):
            log(f"🚫  HTTP {r.status_code} for {day} ({len(ids)} IDs) – skipping block")
            return None

        # ---------- Retry bei 429 ----------
        if r and r.status_code == 429:
            if retries >= MAX_RETRIES:
                log(f"❌  429 after {MAX_RETRIES} retries – giving up day {day}")
                return None
            wait = min(backoff, MAX_BACKOFF)
            log(f"↻  429 Rate-Limit: wait {wait}s …")
            time.sleep(wait)
            retries += 1
            backoff = min(backoff*2, MAX_BACKOFF)
            continue

        # ---------- Retry bei 5xx oder Netzfehler ----------
        if retries >= MAX_RETRIES:
            log(f"❌  failed after {MAX_RETRIES} retries – skipping block {day}")
            return None
        retries += 1
        sleep = 2**retries
        log(f"↻  retry {retries}/{MAX_RETRIES} in {sleep}s …")
        time.sleep(sleep)

def rows_from_json(js: dict, params: list[str]) -> list[list]:
    if not js or not js.get("features"): return []
    dates = [ts[:10] for ts in js["timestamps"]]
    out: list[list] = []
    for feat in js["features"]:
        sid  = feat["properties"]["station"]
        lon, lat = feat["geometry"]["coordinates"]
        pdata = feat["properties"]["parameters"]
        meta  = STATION_META.get(sid, {})
        for i, d in enumerate(dates):
            row = [sid, meta.get("name"), meta.get("state"), lat, lon, d]
            for p in params:
                arr = pdata.get(p, {}).get("data", [])
                row.append(arr[i] if i < len(arr) else None)
            out.append(row)
    return out

def upsert_rows(rows: list[list], cols: list[str]) -> int:
    if not rows: return 0
    sql = textwrap.dedent(f"""
        INSERT INTO daily ({','.join(cols)})
        VALUES ({','.join('%s' for _ in cols)})
        ON CONFLICT (station,date) DO NOTHING
    """)
    with psycopg2.connect(PG_URI) as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
        return cur.rowcount or 0

def export_last7(cols: list[str]):
    cutoff = dt.date.today() - dt.timedelta(days=7)
    with psycopg2.connect(PG_URI) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT {','.join(cols)} FROM daily WHERE date >= %s ORDER BY date,station",
            (cutoff,)
        )
        os.makedirs(SITE_DIR, exist_ok=True)
        with open(f"{SITE_DIR}/last7.csv","w",newline="") as f:
            csv.writer(f).writerows([cols, *cur])

def run_day(day: dt.date, station_ids: list[int], params: list[str]):
    day_rows: list[list] = []
    for block in grouper(station_ids, CHUNK_SIZE):
        js = fetch_json(day, block, params)
        day_rows.extend(rows_from_json(js, params))
    added = upsert_rows(day_rows, META_COLS + params)
    log(f"{day} → {added} rows")

# ─── CLI / Main ───────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="GeoSphere daily loader")
    ap.add_argument("--backfill", nargs=2, metavar=("START","END"),
                    help="ISO dates inclusive")
    ap.add_argument("--stations", nargs="+", type=int,
                    help="restrict to given station IDs")
    ap.add_argument("--parameters", nargs="+",
                    help="restrict to given parameter names (lowercase)")
    return ap.parse_args()

def main() -> None:
    args = parse_args()
    params = args.parameters or PARAMS_DEFAULT
    stations = args.stations or STATIONS_ALL

    if args.backfill:
        start = dt.date.fromisoformat(args.backfill[0])
        end   = dt.date.fromisoformat(args.backfill[1])
        day = start
        log(f"▶️  Backfill {start} … {end}")
        while day <= end:
            run_day(day, stations, params)
            day += dt.timedelta(days=1)
    else:
        run_day(dt.date.today() - dt.timedelta(days=1), stations, params)

    export_last7(META_COLS + params)
    if os.path.exists(TEMPLATE):
        with open(TEMPLATE) as r, open(f"{SITE_DIR}/index.html","w") as w:
            w.write(r.read())
    print("🎉  WetterArena – Build complete")

if __name__ == "__main__":
    main()
