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

# GeoSphere-Limits
MAX_PER_SEC, MAX_PER_HR = 5, 220
HIST_SEC = deque(maxlen=MAX_PER_SEC)
HIST_HR  = deque(maxlen=MAX_PER_HR)
REQ_COUNT = 0

META_COLS    = ["station","name","state","lat","lon","date"]

# ---------------------------------------------------------------------------
# VOLLSTÄNDIGE Parameterliste (131 Kürzel + 131 *_flag = 262 Spalten)
# ---------------------------------------------------------------------------
PARAMS = [
  # Bewölkungs-Dichte & -Menge
  "bewd_i","bewd_i_flag","bewd_ii","bewd_ii_flag","bewd_iii","bewd_iii_flag",
  "bewm_i","bewm_i_flag","bewm_ii","bewm_ii_flag","bewm_iii","bewm_iii_flag",
  "bewm_mittel","bewm_mittel_flag",
  # Sturm­indikatoren
  "bft6","bft6_flag","bft8","bft8_flag",
  # Globalstrahlung
  "cglo_j","cglo_j_flag",
  # Dampfdruck
  "dampf_i","dampf_i_flag","dampf_ii","dampf_ii_flag","dampf_iii","dampf_iii_flag",
  "dampf_mittel","dampf_mittel_flag",
  # Windrichtung (32 Sektoren)
  "dd32_i","dd32_i_flag","dd32_ii","dd32_ii_flag","dd32_iii","dd32_iii_flag",
  # Erdboden­zustand
  "erdb_i","erdb_i_flag","erdb_ii","erdb_ii_flag","erdb_iii","erdb_iii_flag",
  # Windspitzen
  "ffx","ffx_flag",
  # Ereignis-Indikatoren
  "gew","gew_flag","glatt","glatt_flag","nebel","nebel_flag",
  # Luftdruck
  "p_i","p_i_flag","p_ii","p_ii_flag","p_iii","p_iii_flag",
  "p_mittel","p_mittel_flag",
  # Boden­ereignisse
  "raureif","raureif_flag","reif","reif_flag",
  # Relative Feuchte (berechnet aus Feucht-T)
  "rfb_i","rfb_i_flag","rfb_ii","rfb_ii_flag","rfb_iii","rfb_iii_flag",
  "rfb_mittel","rfb_mittel_flag",
  # Relative Feuchte (direkt)
  "rf_i","rf_i_flag","rf_ii","rf_ii_flag","rf_iii","rf_iii_flag",
  "rf_mittel","rf_mittel_flag",
  # Niederschlag
  "rr","rr_flag","rr_i","rr_i_flag","rr_iii","rr_iii_flag",
  "rra_manu","rra_manu_flag","rra_manu_i","rra_manu_i_flag",
  "rra_manu_iii","rra_manu_iii_flag",
  # Schnee
  "sh","sh_flag","sh_manu","sh_manu_flag",
  "sha_manu","sha_manu_flag","shneu_manu","shneu_manu_flag",
  # Sichtweite
  "sicht_i","sicht_i_flag","sicht_ii","sicht_ii_flag","sicht_iii","sicht_iii_flag",
  # Sonne
  "so_h","so_h_flag",
  # Tau
  "tau","tau_flag",
  # Temperatur 2 m
  "tl_i","tl_i_flag","tl_ii","tl_ii_flag","tl_iii","tl_iii_flag",
  "tlmax","tlmax_flag","tlmin","tlmin_flag","tl_mittel","tl_mittel_flag",
  # Temperatur 5 cm
  "tsmin","tsmin_flag",
  # Windstärke (Beaufort)
  "vvbft_i","vvbft_i_flag","vvbft_ii","vvbft_ii_flag","vvbft_iii","vvbft_iii_flag",
  # Windgeschwindigkeit Mittel
  "vv_mittel","vv_mittel_flag",
  # Zeit Spitze
  "zeitx","zeitx_flag"
]

COLS         = META_COLS + PARAMS




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
