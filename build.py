#!/usr/bin/env python3
# build.py – WetterArena v5: Komplettes Backfill + Station‐Meta

import os, sys, csv, datetime as dt, textwrap, requests, psycopg2
from typing import List

# ─────────── Konfiguration ───────────

# Supabase‐URI aus Secret / env
PG_URI    = os.environ["PG_URI"]

# Deine Station-Liste
STATIONS  = [5904, 5925, 5935]

# Station → Name & Bundesland (state)
STATION_META = {
    5904: {"name": "Wien Hohe Warte",   "state": "Wien"},
    5925: {"name": "Wien Innere Stadt", "state": "Wien"},
    5935: {"name": "Wien Donaufeld",    "state": "Wien"},
}

# Dataset und API-Basis
DATASET = "klima-v2-1d"
BASE    = "https://dataset.api.hub.geosphere.at/v1"

# Alle Parameter (kleingeschrieben) + Flags
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

# Meta‐Spalten + Param-Spalten
META_COLS = ["station","name","state","lat","lon","date"]
COLS      = META_COLS + PARAMS

# Für CSV & HTML‐Output
SITE_DIR  = "site"
TEMPLATE  = "index_template.html"
os.makedirs(SITE_DIR, exist_ok=True)

# ──────────────────────────────────────────

def log(*args): 
    print(*args, file=sys.stderr)

# --- GeoSphere API ---

def fetch_json(day: dt.date) -> dict | None:
    url = (
        f"{BASE}/station/historical/{DATASET}"
        f"?start={day}&end={day}"
        f"&station_ids={','.join(map(str,STATIONS))}"
        f"&parameters={','.join(p.upper() for p in PARAMS)}"
    )
    r = requests.get(url, timeout=30)
    return r.json() if r.status_code == 200 else None

def rows_from_json(js: dict) -> List[List]:
    if not js or not js.get("features"):
        return []
    dates = [ts[:10] for ts in js["timestamps"]]
    out   = []
    for feat in js["features"]:
        sid   = feat["properties"]["station"]
        geom  = feat["geometry"]["coordinates"]
        pdata = feat["properties"]["parameters"]
        # Name + state aus Mapping, lat/lon aus geometry
        name  = STATION_META.get(sid, {}).get("name")
        state = STATION_META.get(sid, {}).get("state")
        for i, d in enumerate(dates):
            row = [sid, name, state, geom[0], geom[1], d]
            for p in PARAMS:
                vals = pdata.get(p, {}).get("data", [])
                row.append(vals[i] if i < len(vals) else None)
            out.append(row)
    return out

# --- PostgreSQL Upsert ---

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

# --- CSV‐Export ---

def export_last7():
    cut = dt.date.today() - dt.timedelta(days=7)
    with psycopg2.connect(PG_URI) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT {','.join(COLS)} FROM daily WHERE date >= %s ORDER BY date,station",
            (cut,)
        )
        with open(f"{SITE_DIR}/last7.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(COLS)
            w.writerows(cur)

# --- Tages‐Lauf / Backfill ---

def run_day(day: dt.date):
    rows = rows_from_json(fetch_json(day))
    n    = upsert(rows)
    log(f"{day} → {n} rows")

def main():
    # args: --backfill START END
    if len(sys.argv)==4 and sys.argv[1]=="--backfill":
        start = dt.date.fromisoformat(sys.argv[2])
        end   = dt.date.fromisoformat(sys.argv[3])
        day   = start
        while day <= end:
            run_day(day)
            day += dt.timedelta(days=1)
    else:
        # Standard: immer Vortag laden
        run_day(dt.date.today() - dt.timedelta(days=1))

    # CSV + HTML
    export_last7()
    if os.path.exists(TEMPLATE):
        with open(TEMPLATE) as r, open(f"{SITE_DIR}/index.html","w") as w:
            w.write(r.read())
    print("🎉  WetterArena – Build abgeschlossen.")

if __name__=="__main__":
    main()
