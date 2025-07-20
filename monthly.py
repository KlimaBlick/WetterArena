#!/usr/bin/env python3
# ### paste & run ───────────────────────────────────────────────────────────
"""
monthly.py – WetterArena v10  (Monats-Fetcher mit Bulk-Flush)

• Lädt den 1. Tag jedes Monats aus klima-v2-1m – unverändert.
• Schreibt alle Rows zunächst in einen In-Memory-/Disk-Puffer.
• Ein einziger BULK-INSERT via execute_values() am Ende.
• Fallback:
      – Wenn keine DB-Verbindung möglich: Puffer in CSV sichern.
      – Nächster Start liest CSV ein und flusht sie, bevor neue API-Calls laufen.
"""
from __future__ import annotations
import os, sys, csv, time, random, argparse, datetime as dt, tempfile, atexit, json, pathlib
from collections import deque
from typing import Sequence, List

import requests, psycopg2
from psycopg2.extras import execute_values, register_default_jsonb

# ───── Konfiguration ─────────────────────────────────────────────────────
PG_URI   = os.environ["PG_URI"]
DATASET  = "klima-v2-1m"
BASE     = "https://dataset.api.hub.geosphere.at/v1"
META_CSV = "stations.csv"

CHUNK_SIZE      = 488
MAX_RETRIES     = 3
MAX_WAIT        = 600
MAX_PER_SEC     = 5
MAX_PER_HR      = 220
HIST_SEC, HIST_HR, REQ_COUNT = deque(maxlen=MAX_PER_SEC), deque(maxlen=MAX_PER_HR), 0

META_COLS = ["station", "date"]      # PK = (station, date)
BUFFER_FILE = ".monthly_buffer.csv"  # lokaler Failsafe-Puffer
ROW_BUFFER: list[list] = []          # wächst im RAM, wird bei Bedarf disk-gesichert
MEM_DUMP_THRESHOLD = 250_000         # ab so vielen Zeilen schon im Lauf wegschreiben

# ── Parameterliste ohne *_flag (gekürzt im Beispiel) ────────────────────
PARAMS = [
    # ── Absolute Feuchte (g/m³ ×10) ───────────────────────────────
    "absf_max",
    "absf_min",
    "absf_mittel",

    # ── Äquivalent-/Effektiv-T & Enthalpie & Feuchttemp. ───────────
    "aequi",
    "efftemp",
    "enth",
    "feuchtt",

    # ── Beton 0 cm Lufttemperatur (°C ×10) ─────────────────────────
    "bet0",
    "bet0_max",
    "bet0_min",

    # ── Bewölkungsmenge – Mittelwerte (Zehntel Oktas ×10) ──────────
    "bewm_i_mittel",
    "bewm_ii_mittel",
    "bewm_iii_mittel",
    "bewm_mittel",

    # ── Globalstrahlung (kJ /m²) ───────────────────────────────────
    "cglo_j",

    # ── Dampfdruck – Mittelwerte (hPa ×10) ─────────────────────────
    "dampf_i_mittel",
    "dampf_ii_mittel",
    "dampf_iii_mittel",
    "dampf_mittel",

    # ── Gradtagszahl (20 / 12) ─────────────────────────────────────
    "gradt",

    # ── Erdboden­temperatur 0 cm (°C ×10) ──────────────────────────
    "gras0",
    "gras0_max",
    "gras0_min",

    # ── Mischungsverhältnis – Mittelwert ───────────────────────────
    "misch",

    # ── Luftdruck (hPa ×10) ────────────────────────────────────────
    "p",
    "pmax",
    "pmin",

    # ── Relative Feuchte – Mittelwerte (% ×10) ─────────────────────
    "rf_i_mittel",
    "rf_ii_mittel",
    "rf_iii_mittel",
    "rf_mittel",

    # ── Niederschlag (mm ×10) ──────────────────────────────────────
    "rr",
    "rr_i",
    "rr_iii",
    "rr_max",

    # ── Schnee (cm) ────────────────────────────────────────────────
    "sh_manu_max",
    "shneu_manu",
    "shneu_manu_max",

    # ── Sonnenschein (h ×100 / Soll % ×100) ───────────────────────
    "so_h",
    "so_r",

    # ── Wind – Mittel (m /s ×10) ───────────────────────────────────
    "vv_mittel",

    # ── Tages-Extrema AbsFeuchte / Beton 0 cm ──────────────────────
    "tag_absf_max",
    "tag_absf_min",
    "tag_bet0_max",
    "tag_bet0_min",

    # ── Tage mit Windstille / -richtungen ──────────────────────────
    "tage_ddc",
    "tage_dde",
    "tage_ddn",
    "tage_ddne",
    "tage_ddnw",
    "tage_dds",
    "tage_ddse",
    "tage_ddsw",
    "tage_ddw",

    # ── Ereignis- & Klimatage (Eis, Frost, Gewitter …) ─────────────
    "tage_eis",
    "tage_eschwuel",
    "tage_festrr",
    "tage_festrrp",
    "tage_ffx_100",
    "tage_ffx_60",
    "tage_ffx_70",
    "tage_ffx_80",
    "tage_frost",
    "tage_gew",
    "tage_graupel",
    "tage_hagel",
    "tage_heit",
    "tage_ht",
    "tage_nebel",
    "tage_raureif",
    "tage_reif",
    "tage_rr_01",
    "tage_rr_1",
    "tage_rr_5",
    "tage_rr_10",
    "tage_rr_15",
    "tage_rr_20",
    "tage_rr_30",
    "tage_rr_k1",
    "tage_rr_k2",
    "tage_rr_k3",
    "tage_rr_k4",
    "tage_rr_k5",
    "tage_rr_k6",
    "tage_schdecke",
    "tage_schfall",
    "tage_schoenw",
    "tage_schreg",
    "tage_schwuel",
    "tage_sh_manu_1",
    "tage_sh_manu_5",
    "tage_sh_manu_15",
    "tage_sh_manu_20",
    "tage_sh_manu_30",
    "tage_sh_manu_50",
    "tage_sh_manu_100",
    "tage_sh_manu_k10",
    "tage_sicht_k0",
    "tage_sicht_k1",
    "tage_sicht_k2",
    "tage_sicht_k3",
    "tage_sicht_k7",
    "tage_sicht_k4",
    "tage_sicht_k5",
    "tage_sicht_k6",
    "tage_so_h_0",
    "tage_so_h_1",
    "tage_so_h_5",
    "tage_so_h_10",
    "tage_sommer",
    "tage_stfrost",
    "tage_tau",
    "tage_tl_mittel_10",
    "tage_tl_mittel_15",
    "tage_tl_mittel_20",
    "tage_tl_mittel_25",
    "tage_tl_mittel_k0",
    "tage_tl_mittel_k1",
    "tage_tl_mittel_k2",
    "tage_tl_mittel_k3",
    "tage_tl_mittel_k4",
    "tage_tl_mittel_k5",
    "tage_tl_mittel_k6",
    "tage_tl_mittel_k7",
    "tage_tl_mittel_k8",
    "tage_tl_mittel_k9",
    "tage_tl_mittel_k10",
    "tage_tl_mittel_k16",
    "tage_tl_mittel_m5",
    "tage_tropen",
    "tage_trueb",
    "tage_w6",
    "tage_w8",

    # ── Tages-Maxima / -Minima Niederschlag, Schnee, Boden, Luft ───
    "tag_rr_max",
    "tag_shneu_manu_max",
    "tag_gras0_max",
    "tag_gras0_min",
    "tag_tb2_max",
    "tag_tb2_min",
    "tag_tb5_max",
    "tag_tb5_min",
    "tag_tb10_max",
    "tag_tb10_min",
    "tag_tb15_max",
    "tag_tb15_min",
    "tag_tb20_max",
    "tag_tb20_min",
    "tag_tb30_max",
    "tag_tb30_min",
    "tag_tb40_max",
    "tag_tb40_min",
    "tag_tb50_max",
    "tag_tb50_min",
    "tag_tb70_max",
    "tag_tb70_min",
    "tag_tb100_max",
    "tag_tb100_min",
    "tag_tb110_max",
    "tag_tb110_min",
    "tag_tb200_min",
    "tag_tlmax",
    "tag_tlmin",
    "tag_tsmin",

    # ── Boden-Temperatur – Mittelwerte (°C ×10) ────────────────────
    "tb2_mittel",
    "tb5_mittel",
    "tb10_mittel",
    "tb15_mittel",
    "tb20_mittel",
    "tb30_mittel",
    "tb40_mittel",
    "tb50_mittel",
    "tb70_mittel",
    "tb100_mittel",
    "tb110_mittel",
    "tb200_mittel",

    # ── Boden-Temperatur – Max/Min (°C ×10) ────────────────────────
    "tb2_max",
    "tb2_min",
    "tb5_max",
    "tb5_min",
    "tb10_max",
    "tb10_min",
    "tb15_max",
    "tb15_min",
    "tb20_max",
    "tb20_min",
    "tb30_max",
    "tb30_min",
    "tb40_max",
    "tb40_min",
    "tb50_max",
    "tb50_min",
    "tb70_max",
    "tb70_min",
    "tb100_max",
    "tb100_min",
    "tb110_max",
    "tb110_min",
    "tb200_max",

    # ── Lufttemperatur 2 m – Mittel / Extrema (°C ×10) ─────────────
    "tl_i_mittel",
    "tl_ii_mittel",
    "tl_iii_mittel",
    "tl_mittel",
    "tlmax",
    "tlmax_mittel",
    "tlmin",
    "tlmin_mittel",

    # ── Lufttemperatur 5 cm – Nachtminima (°C ×10) ────────────────
    "tsmin",
    "tsmin_mittel",

    # ── Taupunkt – Mittelwert (°C ×10) ─────────────────────────────
    "tp_mittel",
]


COLS = META_COLS + PARAMS
SQL  = f"INSERT INTO monthly ({','.join(COLS)}) VALUES %s ON CONFLICT (station,date) DO NOTHING"

# ───── Helpers (Monatsarithmetik & Logging) ──────────────────────────────
def month_start(d: dt.date) -> dt.date: return d.replace(day=1)
def add_month(d: dt.date, n: int = 1) -> dt.date:
    y = d.year + (d.month - 1 + n) // 12
    return dt.date(y, (d.month - 1 + n) % 12 + 1, 1)
log = lambda *a: print(*a, file=sys.stderr)

# ───── Ratelimit-Throttle ───────────────────────────────────────────────
def throttle():
    global REQ_COUNT
    now = time.time()
    while HIST_SEC and now - HIST_SEC[0] >= 1:         HIST_SEC.popleft()
    while HIST_HR  and now - HIST_HR[0] >= 3600:       HIST_HR.popleft()
    if len(HIST_SEC) >= MAX_PER_SEC:                   time.sleep(1 - (now-HIST_SEC[0]) + .05)
    if len(HIST_HR)  >= MAX_PER_HR:
        wait = 3600 - (now - HIST_HR[0]) + 1
        if wait > MAX_WAIT: sys.exit("🚫  hourly cap >10 min – abort.")
        print(f"⏸  hourly cap – sleep {wait:.0f}s", file=sys.stderr)
        time.sleep(wait)
    time.sleep(random.uniform(.3, .9))
    REQ_COUNT += 1
    if REQ_COUNT % 10 == 0: time.sleep(random.uniform(2, 4))
def stamp(): t = time.time(); HIST_SEC.append(t); HIST_HR.append(t)
log = lambda *a: print(*a, file=sys.stderr)


# ───── Station-Metadaten laden ───────────────────────────────────────────
def load_stations() -> list[int]:
    ids, today = [], dt.date.today()
    with open(META_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if dt.date.fromisoformat(r["Enddatum"][:10]) >= today:
                ids.append(int(r["id"]))
    return ids
ALL_IDS = load_stations()

# ───── API-Abruf ────────────────────────────────────────────────────────
SESSION = requests.Session(); TIMEOUT = 120
def fetch_json(day: dt.date, ids: Sequence[int]) -> dict|None:
    url = (f"{BASE}/station/historical/{DATASET}"
           f"?start={day}&end={day}"
           f"&station_ids={','.join(map(str,ids))}"
           f"&parameters={','.join(p.upper() for p in PARAMS)}")
    for a in range(MAX_RETRIES + 1):
        throttle()
        try:
            r = SESSION.get(url, timeout=TIMEOUT); stamp()
            if r.status_code == 200: return r.json()
            if r.status_code == 429:
                reset = int(r.headers.get("ratelimit-reset","30"))
                if reset > MAX_WAIT: sys.exit("🚫  API reset >10 min – abort.")
                log(f"↻ 429 – sleep {reset}s"); time.sleep(reset + random.uniform(1,3)); continue
            if a >= MAX_RETRIES: return None
        except (requests.Timeout, requests.ConnectionError): pass
        time.sleep(2 ** a + random.uniform(.5,1))
    return None

# ───── JSON → Rows ──────────────────────────────────────────────────────
def rows_from_json(js: dict) -> list[list]:
    """
    Wandelt das GeoSphere-JSON in eine Zeilenliste um:
    [station_id, date, absf_max, …]  – eine Zeile pro Station × Datum.
    """
    if not js or not js.get("features"):
        return []

    # Zeitstempel in ISO-8601 → date-Objekte (nur YYYY-MM-DD)
    dates = [dt.date.fromisoformat(ts[:10]) for ts in js["timestamps"]]

    rows: list[list] = []
    for feat in js["features"]:
        sid   = feat["properties"]["station"]
        pdata = feat["properties"]["parameters"]

        for i, d in enumerate(dates):
            # Werte in exakt der Reihenfolge aus PARAMS; fehlende => None
            row = [sid, d] + [
                pdata.get(p, {}).get("data", [None] * len(dates))[i]
                for p in PARAMS
            ]
            rows.append(row)

    return rows


# ───── Row-Puffer-Handling ───────────────────────────────────────────────
def buffer_extend(rows: list[list]):
    """Hängt neue Rows an den In-Memory-Puffer an; bei großen Mengen früher Dumps."""
    if not rows: return
    ROW_BUFFER.extend(rows)
    if len(ROW_BUFFER) >= MEM_DUMP_THRESHOLD:
        dump_buffer_to_disk(append=True)      # Teildump, RAM freihalten
        ROW_BUFFER.clear()

def dump_buffer_to_disk(append: bool):
    """Schreibt ROW_BUFFER zeilenweise nach BUFFER_FILE (CSV)."""
    mode = "a" if append and pathlib.Path(BUFFER_FILE).exists() else "w"
    with open(BUFFER_FILE, mode, newline="") as f:
        w = csv.writer(f)
        if mode == "w": w.writerow(COLS)      # Header nur einmal
        w.writerows(ROW_BUFFER)

def load_disk_buffer() -> None:
    """Lädt evtl. vorhandenen Crash-Puffer wieder in ROW_BUFFER."""
    if not pathlib.Path(BUFFER_FILE).exists(): return
    with open(BUFFER_FILE, newline="") as f:
        r = csv.reader(f); next(r, None)      # Header skippen
        ROW_BUFFER.extend([conv_row(row) for row in r])
    os.remove(BUFFER_FILE)
    log(f"↻  Crash-Puffer mit {len(ROW_BUFFER):,} Zeilen wiederhergestellt.")

def conv_row(row: list[str]) -> list:
    """CSV-Strings → passende Python-Types (None, int, float)"""
    out: list = [int(row[0]), dt.date.fromisoformat(row[1])]
    for v in row[2:]:
        if v == "":      out.append(None)
        elif v.isdigit(): out.append(int(v))
        else:            out.append(float(v))
    return out

def flush_to_db():
    """Versucht ROW_BUFFER vollständig in einem Rutsch in die DB zu schreiben."""
    if not ROW_BUFFER: return
    try:
        with psycopg2.connect(PG_URI) as c, c.cursor() as cur:
            execute_values(cur, SQL, ROW_BUFFER, page_size=10_000)
        log(f"✅  {len(ROW_BUFFER):,} Zeilen in Postgres geschrieben.")
        ROW_BUFFER.clear()
    except psycopg2.OperationalError as e:
        log("⚠️  DB-Verbindungsproblem:", e)
        dump_buffer_to_disk(append=False)     # komplette Pufferung
        log(f"💾  {len(ROW_BUFFER):,} Zeilen in {BUFFER_FILE} gesichert – Script endet mit Fehlercode 1.")
        sys.exit(1)

# Flush bei jedem regulären Beenden (auch Ctrl-C) versuchen
atexit.register(flush_to_db)

# ───── Monats-Fetch ──────────────────────────────────────────────────────
def run_month(day: dt.date, ids: list[int], skip: bool) -> bool:
    fails = 0
    for blk in (ids[i:i+CHUNK_SIZE] for i in range(0, len(ids), CHUNK_SIZE)):
        js = fetch_json(day, blk)
        if js is None:
            log("❌  failed block – skip"); fails += 1
            if fails >= 2 and not skip: sys.exit("🚫  two blocks failed – abort.")
            continue
        buffer_extend(rows_from_json(js))
    log(f"{day} ✔  gesammelt (Puffer={len(ROW_BUFFER):,})")
    return fails == 0

# ───── CLI + Ablauf ─────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--backfill", nargs=2, metavar=("START","END"))
    p.add_argument("--stations"); p.add_argument("--skip-ok", action="store_true")
    return p.parse_args()

def main():
    a = parse_args()
    load_disk_buffer()                      # Crash-Reste zuerst flushen

    # Zeitspanne auf Monatsanfänge snappen
    if a.backfill:
        start, end = map(lambda s: month_start(dt.date.fromisoformat(s)), a.backfill)
    else:
        start = end = month_start(dt.date.today() - dt.timedelta(days=32))

    ids = ALL_IDS
    if a.stations:
        ids = [int(x) for x in a.stations.split(",") if x.strip()]
        miss = set(ids) - set(ALL_IDS)
        if miss: log("⚠️  unknown ids ignored:", *miss)

    print(f"▶️  Backfill {start} … {end}")

    cur, consec_fail = start, 0
    while cur <= end:
        ok = run_month(cur, ids, a.skip_ok)
        consec_fail = 0 if ok else consec_fail + 1
        if consec_fail >= 2 and not a.skip_ok:
            sys.exit("🚫  two consecutive months failed – abort.")
        cur = add_month(cur)

    print("🎉  Monthly fetch complete – schreibe in DB …")
    flush_to_db()                          # letzter Versuch; Fehler werden im Handler geloggt

if __name__ == "__main__":
    main()
