#!/usr/bin/env python3
"""
build.py – WetterArena v9.0  (failsafe)

• respektiert GeoSphere-Rate-Limits (5 req/s, 240 req/h)
• CLI-Flags:
      --backfill YYYY-MM-DD YYYY-MM-DD
      --stations  id1,id2,…     (optional)
      --skip-ok                (ignoriert fehlgeschlagene Blöcke; sonst Abbruch)
• bricht NACH ZWEI FEHL­BLÖCKEN IN FOLGE ZWINGEND ab
• schreibt in Supabase-Postgres + site/last7.csv
"""

from __future__ import annotations
import os, sys, csv, time, random, argparse, textwrap, datetime as dt
from collections import deque
from typing import Sequence, List

import requests, psycopg2

# ───── Konfiguration ────────────────────────────────────────────────
PG_URI   = os.environ["PG_URI"]        # postgresql://…
DATASET  = "klima-v2-1d"
BASE     = "https://dataset.api.hub.geosphere.at/v1"
META_CSV = "stations.csv"
SITE_DIR = "site"
TEMPLATE = "index_template.html"

CHUNK_SIZE   = 488           # max. station_ids pro Aufruf
MAX_RETRIES  = 3
MAX_WAIT     = 600           # >10 min  → Abbruch
MAX_FAILS    = 2             
PARAMS       = ["tl_i","tlmax","tlmin","rr","so_h"]   # abruf + DB-Spalten
META_COLS    = ["station","name","state","lat","lon","date"]
COLS         = META_COLS + PARAMS

# GeoSphere-Limits
MAX_PER_SEC, MAX_PER_HR = 5, 220
HIST_SEC = deque(maxlen=MAX_PER_SEC)
HIST_HR  = deque(maxlen=MAX_PER_HR)
REQ_COUNT = 0

# ───── Setup ─────────────────────────────────────────────────────────
os.makedirs(SITE_DIR, exist_ok=True)
SESSION = requests.Session()
TIMEOUT = 120

def throttle():
    """einhält 5 req/s  &  240 req/h + zufällige Jitter-Pausen."""
    global REQ_COUNT
    now = time.time()

    # 5 pro Sek.
    while HIST_SEC and now - HIST_SEC[0] >= 1: HIST_SEC.popleft()
    if len(HIST_SEC) >= MAX_PER_SEC:
        time.sleep(1 - (now - HIST_SEC[0]) + 0.05)

    # 240 pro Std.
    while HIST_HR and now - HIST_HR[0] >= 3600: HIST_HR.popleft()
    if len(HIST_HR) >= MAX_PER_HR:
        wait = 3600 - (now - HIST_HR[0]) + 1
        if wait > MAX_WAIT:
            sys.exit(f"🚫  hourly cap – need {wait:.0f}s (>10 min). Abort.")
        print(f"⏸  hourly cap – sleep {wait:.0f}s", file=sys.stderr)
        time.sleep(wait)

    # „menschliche“ Jitter-Pause
    time.sleep(random.uniform(0.3, 0.9))
    REQ_COUNT += 1
    if REQ_COUNT % 10 == 0:
        time.sleep(random.uniform(2.0, 4.0))

def stamp():
    t = time.time()
    HIST_SEC.append(t)
    HIST_HR.append(t)

def log(*a): print(*a, file=sys.stderr)

# ───── Meta-Daten laden ─────────────────────────────────────────────
def load_stations() -> tuple[list[int], dict[int,dict]]:
    ids, meta = [], {}
    today = dt.date.today()
    with open(META_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if dt.date.fromisoformat(r["Enddatum"][:10]) < today: continue
            sid = int(r["id"]); ids.append(sid)
            meta[sid] = {"name": r["Stationsname"].strip(),
                         "state": r["Bundesland"].strip()}
    return ids, meta

ALL_IDS, META = load_stations()

# ───── Datenabruf ───────────────────────────────────────────────────
def fetch_json(day: dt.date, ids: Sequence[int]) -> dict|None:
    url = (
        f"{BASE}/station/historical/{DATASET}"
        f"?start={day}&end={day}"
        f"&station_ids={','.join(map(str,ids))}"
        f"&parameters={','.join(p.upper() for p in PARAMS)}"
    )
    for attempt in range(MAX_RETRIES + 1):
        throttle()
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            stamp()
            if r.status_code == 200:
                return r.json()

            if r.status_code == 429:
                reset = int(r.headers.get("ratelimit-reset", "30"))
                if reset > MAX_WAIT:
                    sys.exit(f"🚫  API reset {reset}s (>10 min) – abort.")
                log(f"↻ 429 – sleep {reset}s")
                time.sleep(reset + random.uniform(1,3))
                continue            # noch ein Versuch

            # 4xx/5xx → Retry-Backoff
            back = 2 ** attempt
            if attempt >= MAX_RETRIES:
                return None
            log(f"↻  HTTP {r.status_code} – retry {attempt+1} in {back}s")
            time.sleep(back + random.uniform(0.5,1.0))

        except (requests.Timeout,
                requests.ConnectionError,
                requests.HTTPError) as ex:
            back = 2 ** attempt
            if attempt >= MAX_RETRIES:
                return None
            log(f"↻  {ex.__class__.__name__} – retry {attempt+1} in {back}s")
            time.sleep(back + random.uniform(0.5,1.0))
    return None

def rows_from_json(js: dict) -> List[List]:
    if not js or not js.get("features"): return []
    dates = [ts[:10] for ts in js["timestamps"]]
    out   = []
    for feat in js["features"]:
        sid, coords = feat["properties"]["station"], feat["geometry"]["coordinates"]
        pdata = feat["properties"]["parameters"]
        meta  = META.get(sid, {})
        for i, d in enumerate(dates):
            row = [sid, meta.get("name"), meta.get("state"), coords[0], coords[1], d]
            for p in PARAMS:
                arr = pdata.get(p, {}).get("data", [])
                row.append(arr[i] if i < len(arr) else None)
            out.append(row)
    return out

SQL = textwrap.dedent(f"""
    INSERT INTO daily ({','.join(COLS)})
    VALUES ({','.join('%s' for _ in COLS)})
    ON CONFLICT (station,date) DO NOTHING
""")

def upsert(rows: List[List]) -> int:
    if not rows: return 0
    with psycopg2.connect(PG_URI) as conn, conn.cursor() as cur:
        cur.executemany(SQL, rows)
        return cur.rowcount or 0

def export_last7():
    cutoff = dt.date.today() - dt.timedelta(days=7)
    with psycopg2.connect(PG_URI) as c, c.cursor() as cur:
        cur.execute(f"SELECT {','.join(COLS)} FROM daily WHERE date >= %s ORDER BY date,station", (cutoff,))
        with open(f"{SITE_DIR}/last7.csv","w",newline="") as f:
            w = csv.writer(f); w.writerow(COLS); w.writerows(cur)

# ───── Hauptablauf pro Tag ──────────────────────────────────────────
def run_day(day: dt.date, ids: list[int], skip_ok: bool) -> bool:
    """True = ok, False = komplett gescheitert (≥1 Block)"""
    failed_blocks = 0
    for block in (ids[i:i+CHUNK_SIZE] for i in range(0,len(ids),CHUNK_SIZE)):
        js = fetch_json(day, block)
        if js is None:
            log("❌  failed block – skip")
            failed_blocks += 1
            if failed_blocks >= MAX_FAILS and not skip_ok:
                sys.exit("🚫  two blocks failed – aborting whole script.")
            continue
        n = upsert(rows_from_json(js))
        log(f"{day} → {n} rows")
    return failed_blocks == 0

# ───── CLI & Steuerlogik ────────────────────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", nargs=2, metavar=("START","END"))
    ap.add_argument("--stations", help="id1,id2,…")
    ap.add_argument("--skip-ok", action="store_true",
                    help="bei Block-Fehlern nur Tag überspringen")
    return ap.parse_args()

def main():
    args = parse_args()

    # Datumsbereich bestimmen
    if args.backfill:
        start, end = map(dt.date.fromisoformat, args.backfill)
    else:
        start = end = dt.date.today() - dt.timedelta(days=1)

    # Stationsauswahl
    ids = ALL_IDS
    if args.stations:
        ids = [int(x) for x in args.stations.split(",") if x.strip()]
        missing = set(ids) - set(ALL_IDS)
        if missing: log("⚠️  unknown ids ignored:", *missing)

    print(f"▶️  Backfill {start} … {end}  ({(end-start).days+1} days)")

    consec_fail_days = 0
    cur = start
    while cur <= end:
        ok = run_day(cur, ids, args.skip_ok)
        if ok:
            consec_fail_days = 0
        else:
            consec_fail_days += 1
            if consec_fail_days >= MAX_FAILS and not args.skip_ok:
                sys.exit("🚫  two consecutive days failed – abort.")
        cur += dt.timedelta(days=1)

    export_last7()
    if os.path.exists(TEMPLATE):
        with open(TEMPLATE) as r, open(f"{SITE_DIR}/index.html","w") as w: w.write(r.read())
    print("🎉  Build complete.")

if __name__ == "__main__":
    main()
