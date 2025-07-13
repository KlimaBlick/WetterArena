#!/usr/bin/env python3
"""
build.py – WetterArena v8.1
• Liest station metadata (ID, Stationsname, Bundesland, Enddatum) aus stations.csv
• Lädt täglich (oder per --backfill START END) alle aktiven Stationen in 30-Tage-Blöcken
• Verwendet längeren Timeout + Retries
• Schreibt in Supabase (Postgres) und generiert site/last7.csv + site/index.html
"""

import os
import sys
import csv
import time
import datetime as dt
import textwrap
import requests
import psycopg2
from typing import List

# ─── Konfiguration ────────────────────────────────────────────

PG_URI     = os.environ["PG_URI"]      # z.B. postgresql://postgres:<PW>@db.xxx.supabase.co:5432/postgres
META_CSV   = "stations.csv"            # Deine Stations-Metadaten
DATASET    = "klima-v2-1d"
BASE       = "https://dataset.api.hub.geosphere.at/v1"
SITE_DIR   = "site"
TEMPLATE   = "index_template.html"

# Parameter (kleingeschrieben) + Flags
PARAMS = [
  "bewd_i","bewd_i_flag","bewd_ii","bewd_ii_flag","bewd_iii","bewd_iii_flag",
  "bewm_i","bewm_i_flag","bewm_ii","bewm_ii_flag","bewm_iii","bewm_iii_flag",
  "bewm_mittel","bewm_mittel_flag","bft6","bft6_flag","bft8","bft8_flag",
  "cglo_j","cglo_j_flag","dampf_i","dampf_i_flag","dampf_ii","dampf_ii_flag",
  "dampf_iii","dampf_iii_flag","dampf_mittel","dampf_mittel_flag",
  "dd32_i","dd32_i_flag","dd32_ii","dd32_ii_flag","dd32_iii","dd32_iii_flag",
  "erdb_i","erdb_i_flag","erdb_ii","erdb_ii_flag","erdb_iii","erdb_iii_flag",
  "ffx","ffx_flag","gew","gew_flag","glatt","glatt_flag","nebel","nebel_flag",
  "p_i","p_i_flag","p_ii","p_ii_flag","p_iii","p_iii_flag",
  "p_mittel","p_mittel_flag","raureif","raureif_flag","reif","reif_flag",
  "rfb_i","rfb_i_flag","rfb_ii","rfb_ii_flag","rfb_iii","rfb_iii_flag",
  "rfb_mittel","rfb_mittel_flag","rf_i","rf_i_flag","rf_ii","rf_ii_flag",
  "rf_iii","rf_iii_flag","rf_mittel","rf_mittel_flag",
  "rr","rr_flag","rr_i","rr_i_flag","rr_iii","rr_iii_flag",
  "rra_manu","rra_manu_flag","rra_manu_i","rra_manu_i_flag",
  "rra_manu_iii","rra_manu_iii_flag",
  "sh","sh_flag","sh_manu","sh_manu_flag","sha_manu","sha_manu_flag",
  "shneu_manu","shneu_manu_flag","sicht_i","sicht_i_flag",
  "sicht_ii","sicht_ii_flag","sicht_iii","sicht_iii_flag",
  "so_h","so_h_flag","tau","tau_flag",
  "tl_i","tl_i_flag","tl_ii","tl_ii_flag","tl_iii","tl_iii_flag",
  "tlmax","tlmax_flag","tlmin","tlmin_flag","tl_mittel","tl_mittel_flag",
  "tsmin","tsmin_flag","vvbft_i","vvbft_i_flag","vvbft_ii","vvbft_ii_flag",
  "vvbft_iii","vvbft_iii_flag","vv_mittel","vv_mittel_flag","zeitx","zeitx_flag"
]

META_COLS = ["station","name","state","lat","lon","date"]
COLS      = META_COLS + PARAMS

os.makedirs(SITE_DIR, exist_ok=True)

# ─── Hilfsfunktionen ──────────────────────────────────────────

def log(*args):
    print(*args, file=sys.stderr)

def load_stations(meta_csv=META_CSV) -> tuple[List[int], dict]:
    """
    Liest stations.csv mit Spalten:
      id, Stationsname, Bundesland, Enddatum (z.B. '2100-12-31T00:00:00+00:00')
    Liefert:
      STATIONS: IDs mit Enddatum >= heute
      STATION_META: {id:{"name":..., "state":...}}
    """
    stations = []
    mapping  = {}
    today = dt.date.today()
    with open(meta_csv, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            end_raw = row.get("Enddatum", "")[:10]
            try:
                valid_to = dt.date.fromisoformat(end_raw)
            except:
                continue
            if valid_to < today:
                continue
            sid = int(row["id"])
            stations.append(sid)
            mapping[sid] = {
                "name":  row.get("Stationsname", "").strip(),
                "state": row.get("Bundesland", "").strip()
            }
    return stations, mapping

STATIONS, STATION_META = load_stations()

def fetch_json(day: dt.date) -> dict | None:
    """
    Holt JSON für einen Tag mit Retries & Timeout=120s
    """
    url = (
        f"{BASE}/station/historical/{DATASET}"
        f"?start={day}&end={day}"
        f"&station_ids={','.join(map(str,STATIONS))}"
        f"&parameters={','.join(p.upper() for p in PARAMS)}"
    )
    backoff = 1
    for _ in range(5):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                return r.json()
            log(f"⚠️ HTTP {r.status_code} für {day}")
            return None
        except requests.exceptions.ReadTimeout:
            log(f"⏱️ Timeout für {day}, retry in {backoff}s…")
            time.sleep(backoff)
            backoff *= 2
    log(f"❌ Keine Daten für {day} nach 5 Versuchen")
    return None

def rows_from_json(js: dict) -> List[List]:
    if not js or not js.get("features"):
        return []
    dates = [ts[:10] for ts in js["timestamps"]]
    out   = []
    for feat in js["features"]:
        sid    = feat["properties"]["station"]
        coords = feat["geometry"]["coordinates"]
        pdata  = feat["properties"]["parameters"]
        meta   = STATION_META.get(sid, {})
        name   = meta.get("name")
        state  = meta.get("state")
        for i, d in enumerate(dates):
            row = [sid, name, state, coords[0], coords[1], d]
            for p in PARAMS:
                arr = pdata.get(p, {}).get("data", [])
                row.append(arr[i] if i < len(arr) else None)
            out.append(row)
    return out

INSERT_SQL = textwrap.dedent(f"""
    INSERT INTO daily ({','.join(COLS)})
    VALUES ({','.join('%s' for _ in COLS)})
    ON CONFLICT (station,date) DO NOTHING
""")

def upsert(rows: List[List]) -> int:
    if not rows:
        return 0
    with psycopg2.connect(PG_URI) as conn, conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
        return cur.rowcount or 0

def export_last7():
    cutoff = dt.date.today() - dt.timedelta(days=7)
    with psycopg2.connect(PG_URI) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT {','.join(COLS)} FROM daily WHERE date >= %s ORDER BY date,station",
            (cutoff,)
        )
        with open(f"{SITE_DIR}/last7.csv","w",newline="") as f:
            w = csv.writer(f)
            w.writerow(COLS)
            w.writerows(cur)

def run_day(day: dt.date):
    rows = rows_from_json(fetch_json(day))
    n    = upsert(rows)
    log(f"{day} → {n} Zeilen")

# ─── Main ─────────────────────────────────────────────────────

def main():
    if len(sys.argv)==4 and sys.argv[1]=="--backfill":
        start = dt.date.fromisoformat(sys.argv[2])
        end   = dt.date.fromisoformat(sys.argv[3])
        chunk = start
        while chunk <= end:
            block_end = min(end, chunk + dt.timedelta(days=29))
            log(f"▶️ Backfill {chunk} bis {block_end}")
            day = chunk
            while day <= block_end:
                run_day(day)
                day += dt.timedelta(days=1)
            chunk = block_end + dt.timedelta(days=1)
    else:
        run_day(dt.date.today() - dt.timedelta(days=1))

    export_last7()
    if os.path.exists(TEMPLATE):
        with open(TEMPLATE) as r, open(f"{SITE_DIR}/index.html","w") as w:
            w.write(r.read())
    print("🎉  WetterArena – Build abgeschlossen.")

if __name__ == "__main__":
    main()
