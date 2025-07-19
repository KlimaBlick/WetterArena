#!/usr/bin/env python3
# ### paste & run ────────────────────────────────────────────────────────────
"""
build.py – WetterArena v9.1 (flags + Metadaten entfernt)

• respektiert GeoSphere-Rate-Limits (5 req/s, 220 req/h)
• CLI-Flags:
      --backfill YYYY-MM-DD YYYY-MM-DD
      --stations  id1,id2,…     (optional)
      --skip-ok                (ignoriert fehlgeschlagene Blöcke; sonst Abbruch)
• bricht NACH ZWEI FEHLBLÖCKEN IN FOLGE zwingend ab
• schreibt in Supabase-Postgres + site/last7.csv
"""

from __future__ import annotations
import os, sys, csv, time, random, argparse, textwrap, datetime as dt
from collections import deque
from typing import Sequence, List

import requests, psycopg2
from psycopg2.extras import execute_values              # <-- schneller Bulk-Insert

# ───── Konfiguration ───────────────────────────────────────────────────────
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

# GeoSphere-Limits
MAX_PER_SEC, MAX_PER_HR = 5, 220
HIST_SEC = deque(maxlen=MAX_PER_SEC)
HIST_HR  = deque(maxlen=MAX_PER_HR)
REQ_COUNT = 0

META_COLS = ["station", "date"]        # <-- nur noch 2 Spalten

# ---------------------------------------------------------------------------
# Parameterliste (alle *_flag entfernt, 131 Spalten)
# ---------------------------------------------------------------------------
PARAMS = [
    # Bewölkungs-Dichte & -Menge
    "bewd_i", "bewd_ii", "bewd_iii",
    "bewm_i", "bewm_ii", "bewm_iii", "bewm_mittel",
    # Sturmindikatoren
    "bft6", "bft8",
    # Globalstrahlung
    "cglo_j",
    # Dampfdruck
    "dampf_i", "dampf_ii", "dampf_iii", "dampf_mittel",
    # Windrichtung (32 Sektoren)
    "dd32_i", "dd32_ii", "dd32_iii",
    # Erdbodenzustand
    "erdb_i", "erdb_ii", "erdb_iii",
    # Windspitzen
    "ffx",
    # Ereignis-Indikatoren
    "gew", "glatt", "nebel",
    # Luftdruck
    "p_i", "p_ii", "p_iii", "p_mittel",
    # Bodenergebnisse
    "raureif", "reif",
    # Relative Feuchte (berechnet)
    "rfb_i", "rfb_ii", "rfb_iii", "rfb_mittel",
    # Relative Feuchte (direkt)
    "rf_i", "rf_ii", "rf_iii", "rf_mittel",
    # Niederschlag
    "rr", "rr_i", "rr_iii",
    "rra_manu", "rra_manu_i", "rra_manu_iii",
    # Schnee
    "sh", "sh_manu", "sha_manu", "shneu_manu",
    # Sichtweite
    "sicht_i", "sicht_ii", "sicht_iii",
    # Sonne
    "so_h",
    # Tau
    "tau",
    # Temperatur 2 m
    "tl_i", "tl_ii", "tl_iii",
    "tlmax", "tlmin", "tl_mittel",
    # Temperatur 5 cm
    "tsmin",
    # Windstärke (Beaufort)
    "vvbft_i", "vvbft_ii", "vvbft_iii",
    # Windgeschwindigkeit Mittel
    "vv_mittel",
    # Zeit Spitze
    "zeitx"
]

COLS = META_COLS + PARAMS

# ───── Setup ───────────────────────────────────────────────────────────────
os.makedirs(SITE_DIR, exist_ok=True)
SESSION = requests.Session()
TIMEOUT = 120

def throttle():
    """hält 5 req/s & 220 req/h + Jitter ein."""
    global REQ_COUNT
    now = time.time()

    # 5 pro Sekunde
    while HIST_SEC and now - HIST_SEC[0] >= 1: HIST_SEC.popleft()
    if len(HIST_SEC) >= MAX_PER_SEC:
        time.sleep(1 - (now - HIST_SEC[0]) + 0.05)

    # 220 pro Stunde
    while HIST_HR and now - HIST_HR[0] >= 3600: HIST_HR.popleft()
    if len(HIST_HR) >= MAX_PER_HR:
        wait = 3600 - (now - HIST_HR[0]) + 1
        if wait > MAX_WAIT:
            sys.exit(f"🚫  hourly cap – need {wait:.0f}s (>10 min). Abort.")
        print(f"⏸  hourly cap – sleep {wait:.0f}s", file=sys.stderr)
        time.sleep(wait)

    # zufällige Pause
    time.sleep(random.uniform(0.3, 0.9))
    REQ_COUNT += 1
    if REQ_COUNT % 10 == 0:
        time.sleep(random.uniform(2.0, 4.0))

def stamp():
    t = time.time()
    HIST_SEC.append(t)
    HIST_HR.append(t)

def log(*a): print(*a, file=sys.stderr)

# ───── Stations-Meta laden ────────────────────────────────────────────────
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

# ───── Datenabruf ─────────────────────────────────────────────────────────
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
                continue

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

# ───── JSON → Zeilen ─────────────────────────────────────────────────────
def rows_from_json(js: dict) -> List[List]:
    if not js or not js.get("features"): return []
    dates = [dt.date.fromisoformat(ts[:10]) for ts in js["timestamps"]]   # date-Objekte
    out   = []
    for feat in js["features"]:
        sid, pdata = feat["properties"]["station"], feat["properties"]["parameters"]
        for i, d in enumerate(dates):
            row = [sid, d]                           # <-- nur station + date
            for p in PARAMS:
                arr = pdata.get(p, {}).get("data", [])
                row.append(arr[i] if i < len(arr) else None)
            out.append(row)
    return out

SQL = f"""
    INSERT INTO daily ({','.join(COLS)})
    VALUES %s
    ON CONFLICT (station,date) DO NOTHING
"""

def upsert(rows: List[List]) -> int:
    if not rows: 
        return 0
    with psycopg2.connect(PG_URI) as conn, conn.cursor() as cur:
        execute_values(cur, SQL, rows, page_size=1000)      # <-- Bulk-Insert
        return cur.rowcount or 0

# ───── Last 7 Tage exportieren ────────────────────────────────────────────
def export_last7():
    cutoff = dt.date.today() - dt.timedelta(days=7)
    with psycopg2.connect(PG_URI) as c, c.cursor() as cur:
        cur.execute(f"SELECT {','.join(COLS)} FROM daily WHERE date >= %s ORDER BY date,station", (cutoff,))
        with open(f"{SITE_DIR}/last7.csv","w",newline="") as f:
            w = csv.writer(f); w.writerow(COLS); w.writerows(cur)

# ───── Haupt-Loop pro Tag ────────────────────────────────────────────────
def run_day(day: dt.date, ids: list[int], skip_ok: bool) -> bool:
    failed = 0
    for block in (ids[i:i+CHUNK_SIZE] for i in range(0,len(ids),CHUNK_SIZE)):
        js = fetch_json(day, block)
        if js is None:
            log("❌  failed block – skip")
            failed += 1
            if failed >= MAX_FAILS and not skip_ok:
                sys.exit("🚫  two blocks failed – aborting script.")
            continue
        n = upsert(rows_from_json(js))
        log(f"{day} → {n} rows")
    return failed == 0

# ───── CLI & Ablaufsteuerung ─────────────────────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", nargs=2, metavar=("START","END"))
    ap.add_argument("--stations")
    ap.add_argument("--skip-ok", action="store_true")
    return ap.parse_args()

def main():
    args = parse_args()

    # Datumsbereich
    if args.backfill:
        start, end = map(dt.date.fromisoformat, args.backfill)
    else:
        start = end = dt.date.today() - dt.timedelta(days=1)

    # Stationsfilter
    ids = ALL_IDS
    if args.stations:
        ids = [int(x) for x in args.stations.split(",") if x.strip()]
        missing = set(ids) - set(ALL_IDS)
        if missing: log("⚠️  unknown ids ignored:", *missing)

    print(f"▶️  Backfill {start} … {end}  ({(end-start).days+1} days)")

    consec_fail = 0
    cur = start
    while cur <= end:
        ok = run_day(cur, ids, args.skip_ok)
        consec_fail = 0 if ok else consec_fail + 1
        if consec_fail >= MAX_FAILS and not args.skip_ok:
            sys.exit("🚫  two consecutive days failed – abort.")
        cur += dt.timedelta(days=1)

    export_last7()
    if os.path.exists(TEMPLATE):
        with open(TEMPLATE) as r, open(f"{SITE_DIR}/index.html","w") as w:
            w.write(r.read())
    print("🎉  Build complete.")

if __name__ == "__main__":
    main()
