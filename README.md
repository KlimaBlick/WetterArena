# KlimaBlick – schlanke Wetter-Visualisierung (v0.3)

> **Live-Demo:** <https://klimablick.github.io/WetterArena/>  
> Daten © GeoSphere Austria · CC-BY 4.0

## Architektur in 60 Sekunden
| Schicht | Technologie | Aufgabe |
|---------|-------------|---------|
| **Collector** | `collector.py`, `requests` | holt 10-Min-JSON von GeoSphere API |
| **DB** | Supabase Postgres<br>+ (optional) Timescale / Columnar Store | speichert Roh-Messwerte *(raw_10m)* und Tagesverdichtungen *(daily)* |
| **Builder** | `build.py`, `psycopg2` | verdichtet letzte 7 Tage → `site/last7.csv`, rendert `index.html` |
| **Hosting** | GitHub Actions + Pages | cron `*/10 * * * *` → statisches Frontend |

## Quick-Start (local)
```bash
git clone https://github.com/KlimaBlick/WetterArena.git
cd WetterArena
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export PG_URI="postgresql://<user>:<pw>@<POOLER_HOST>:5432/postgres"
python collector.py --backfill 2025-01-01 2025-01-31
python build.py
open site/index.html           # Windows: start site\index.html
GitHub Secrets
Name	Wert	Hinweis
PG_URI	postgresql://<user>:<pw>@<POOLER_HOST>:5432/postgres	POOLER_HOST = Supabase → Connection Pooling / IPv4
SUPABASE_SERVICE_KEY (optional)	ey...	nur falls Policies o. RLS via Skript gemanagt

Datenbank-Schema (kompakt)
sql
Copy
Edit
-- Roh-Tabelle (≈ 50 Mio Zeilen/Jahr)
CREATE UNLOGGED TABLE raw_10m (
  station  int,
  ts_utc   timestamptz,
  tl       real,
  rr       real,
  sd       real,
  PRIMARY KEY (station, ts_utc)
);
/* komprimierte Tages-View */
CREATE MATERIALIZED VIEW daily AS
SELECT
  station,
  date_trunc('day', ts_utc)::date AS date,
  max(tl) AS tmax,
  min(tl) AS tmin,
  sum(rr) AS rr,
  sum(sd) AS sd
FROM raw_10m
GROUP BY 1,2;
CREATE UNIQUE INDEX ON daily (station, date);
Workflow .github/workflows/nightly-build.yml
yaml
Copy
Edit
name: nightly-build
on:
  schedule:         # alle 10 Minuten
    - cron: '*/10 * * * *'
  workflow_dispatch:
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install -r requirements.txt
      - run: python collector.py
        env: { PG_URI: ${{ secrets.PG_URI }} }
      - run: python build.py
        env: { PG_URI: ${{ secrets.PG_URI }} }
      - uses: peaceiris/actions-gh-pages@v4
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: site
Roadmap
Timescale Kompression aktivieren → 80 % Platzersparnis.

Leaflet-Heatmap als Option für High-Res-Karten.

Web Push “Regenwarnung” (Supabase → Edge Functions).

FERTIG.
Damit hast du einmal das minimale ChatGPT-Briefing und ein eins-zu-eins kopierbares README.
Alles Weitere gern Schritt für Schritt hier im Chat!




Sources
You said:
Bis Roadmap "Roadmap
Timescale Kompression aktivieren → 80 % Platzersparnis.

Leaflet-Heatmap als Option für High-Res-Karten.

Web Push “Regenwarnung” (Supabase → Edge Functions)." Kommt alles ins Readme. Auch das Datenbank Schema? Und warum auf GitHub? Warum nicht als Dokument ins ChatGPT Projekt?


ChatGPT said:

Thought for 7 seconds
Kurz & klar

Datei	Zweck	Wer greift darauf zu?
README.md (im Repo)	Single Source of Truth für alle Tech-Infos: Architektur ➜ Setup ➜ DB-Schema ➜ CI Workflow ➜ Roadmap	jeder, der das Repo klont (GitHub UI, VS Code, etc.)
“Project-Instructions” (ChatGPT-Popup)	Mini-Briefing, damit ChatGPT in jeder Session sofort den Kontext kennt (ohne erst dein README zu parsen).	nur du + ChatGPT