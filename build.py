#!/usr/bin/env python3
"""
build.py – WetterArena v8.2.1  (fail-fast & CLI-Optionen)

• Lädt GeoSphere-Tagesdaten (DATASET = klima-v2-1d) für alle aktiven Stationen
  oder – per CLI – für eine Teilmenge.
• Schreibt in Supabase (PostgreSQL) und erzeugt site/last7.csv (+ index.html).
• Bricht sofort ab, wenn der Server 403/404 liefert, statt minutenlang zu retryen.

Usage-Beispiele
---------------
# Gestern aktualisieren (Default)
python build.py

# Backfill eines Zeitraums
python build.py --backfill 2025-01-01 2025-01-31

# Nur eine Station und 2 Parameter testen
python build.py --stations 11035 --parameters tl_i rr --backfill 2025-01-07 2025-01-07
"""

from __future__ import annotations
import os, sys, csv, time, textwrap, argparse
import datetime as dt
from typing import List, Iterable
import requests, psycopg2

# ─── Konfiguration ────────────────────────────────────────────
PG_URI       = os.environ["PG_URI"]          # z.B. postgresql://postgres:<PW>@db.xxx.supabase.co:5432/postgres
DATASET      = "klima-v2-1d"
BASE         = "https://dataset.api.hub.geosphere.at/v1"
META_CSV     = "stations.csv"
SITE_DIR     = "site"
TEMPLATE     = "index_template.html"

CHUNK_SIZE   = 50        # Stationen pro Request   (klein halten!)
MAX_RETRIES  = 2         # 1 + 1 Retry bei 5xx / Timeout
TIMEOUT      = 90        # Sekunden pro Request

PARAMS_DEFAULT = [
  "tl_i","tl_ii","tl_iii","tlmax","tlmin",
  "rr","rr_i","rr_iii",
  "so_h","so_h_flag",
  # … kürzen nach Bedarf …
]

META_COLS = ["station","name","state","lat","lon","date"]

# ─── Hilfs-Funktionen ─────────────────────────────────────────
def log(*a): print(*a, file=sys.stderr)

def load_stations(meta_csv=META_CSV) -> tuple[list[int], dict[int,dict]]:
    stations, meta = [], {}
    today = dt.date.today()
    with open(meta_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                valid_to = dt.date.fromisoformat(row.get("Enddatum", "")[:10])
            except ValueError:
                continue
            if valid_to >= today:
                sid = int(row["id"])
                stations.append(sid)
                meta[sid] = {"name": row["Stationsname"].strip(),
                             "state": row["Bundesland"].strip()}
    return stations, meta

STATIONS_ALL, ST_META = load_stations()

def grouper(n: int, iterable: Iterable[int]) -> Iterable[list[int]]:
    "yield lists of length ≤ n"
    block: list[int] = []
    for item in iterable:
        block.append(item)
        if len(block) == n:
            yield block; block = []
    if block: yield block

def fetch_json(day: dt.date, station_ids: list[int], params: list[str]) -> dict|None:
    url = (f"{BASE}/station/historical/{DATASET}"
           f"?start={day}&end={day}"
           f"&station_ids={','.join(map(str,station_ids))}"
           f"&parameters={','.join(p.upper() for p in params)}")

    backoff = 1
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()

            if r.status_code in (403, 404):
                log(f"🚫 HTTP {r.status_code} – {url}")
                sys.exit(1)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", backoff))
                log(f"↻ 429 Rate-Limit: warten {wait}s …"); time.sleep(wait)
            elif 500 <= r.status_code < 600:
                log(f"⚠️ {r.status_code} Server-Error – Retry in {backoff}s")
            else:
                log(f"⚠️ HTTP {r.status_code} – Retry")
        except (requests.Timeout, requests.ConnectionError) as e:
            log(f"⏱️ {type(e).__name__} – Retry in {backoff}s")
        time.sleep(backoff); backoff *= 2
    log(f"❌  {day}: keine Daten nach {MAX_RETRIES} Retries")
    return None

def rows_from_json(js: dict, params: list[str]) -> list[list]:
    if not js or not js.get("features"): return []
    dates = [ts[:10] for ts in js["timestamps"]]
    rows  = []
    for feat in js["features"]:
        sid, coords = feat["properties"]["station"], feat["geometry"]["coordinates"]
        pdata       = feat["properties"]["parameters"]
        meta        = ST_META.get(sid, {})
        for i, d in enumerate(dates):
            row = [sid, meta.get("name"), meta.get("state"), coords[0], coords[1], d]
            for p in params:
                arr = pdata.get(p, {}).get("data", [])
                row.append(arr[i] if i < len(arr) else None)
            rows.append(row)
    return rows

def upsert(rows: list[list], cols: list[str]) -> int:
    if not rows: return 0
    sql = (f"INSERT INTO daily ({','.join(cols)}) VALUES "
           f"({','.join('%s' for _ in cols)}) ON CONFLICT (station,date) DO NOTHING")
    with psycopg2.connect(PG_URI) as conn, conn.cursor() as cur:
        cur.executemany(sql, rows); return cur.rowcount or 0

def export_last7(cols: list[str]):
    cutoff = dt.date.today() - dt.timedelta(days=7)
    with psycopg2.connect(PG_URI) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {','.join(cols)} FROM daily WHERE date >= %s "
                    "ORDER BY date,station", (cutoff,))
        os.makedirs(SITE_DIR, exist_ok=True)
        with open(f"{SITE_DIR}/last7.csv","w",newline="") as f:
            w = csv.writer(f); w.writerow(cols); w.writerows(cur)

def run_day(day: dt.date, station_ids: list[int], params: list[str], cols: list[str]):
    total = 0
    for block in grouper(CHUNK_SIZE, station_ids):
        js   = fetch_json(day, block, params)
        rows = rows_from_json(js, params)
        total += upsert(rows, cols)
    log(f"{day} → {total} Zeilen")

# ─── main() ───────────────────────────────────────────────────
def parse_cli():
    ap = argparse.ArgumentParser(description="GeoSphere → Supabase Loader")
    ap.add_argument("--backfill", nargs=2, metavar=("START","END"),
                    help="Zeitraum yyyy-mm-dd yyyy-mm-dd")
    ap.add_argument("--stations",  help="Komma-Liste Station-IDs")
    ap.add_argument("--parameters", help="Parameter, z. B. tl_i,rr")
    return ap.parse_args()

def main():
    args = parse_cli()

    stations = (list(map(int, args.stations.split(",")))
                if args.stations else STATIONS_ALL)
    params   = (args.parameters.lower().split(",")
                if args.parameters else PARAMS_DEFAULT)
    cols     = META_COLS + params

    if args.backfill:
        start = dt.date.fromisoformat(args.backfill[0])
        end   = dt.date.fromisoformat(args.backfill[1])
        log(f"▶️ Backfill {start} … {end}")
        for day in (start + dt.timedelta(n) for n in range((end-start).days+1)):
            run_day(day, stations, params, cols)
    else:
        run_day(dt.date.today() - dt.timedelta(days=1), stations, params, cols)

    export_last7(cols)
    if os.path.exists(TEMPLATE):
        with open(TEMPLATE) as r, open(f"{SITE_DIR}/index.html","w") as w:
            w.write(r.read())
    print("🎉  WetterArena – Build abgeschlossen.")

if __name__ == "__main__":
    main()
