"""
Agent de détection des changements de dirigeants
PME industrielles — Île-de-France
Tourne chaque jour via GitHub Actions
"""

import requests
import pandas as pd
import time
import os
import json
import gspread
from datetime import datetime
from pathlib import Path
from google.oauth2.service_account import Credentials
from tqdm import tqdm

# ============================================================
# CONFIGURATION
# ============================================================

BASE_URL             = "https://recherche-entreprises.api.gouv.fr/search"
REGION_IDF           = "11"
CATEGORIE            = "PME"
DELAY_BETWEEN_CALLS  = 1.0
PER_PAGE             = 25

SNAPSHOT_DIR = Path("snapshots")
SNAPSHOT_DIR.mkdir(exist_ok=True)

GOOGLE_SHEET_NAME = "Agent Dirigeants IDF"

CODES_NAF = [
    "10.11Z", "10.12Z", "10.13A", "10.13B", "10.20Z", "10.31Z", "10.32Z", "10.39A", "10.39B",
    "10.41A", "10.41B", "10.42Z", "10.51A", "10.51B", "10.51C", "10.51D", "10.52Z",
    "10.61A", "10.61B", "10.62Z", "10.71A", "10.71B", "10.71C", "10.71D", "10.72Z",
    "10.73Z", "10.81Z", "10.82Z", "10.83Z", "10.84Z", "10.85Z", "10.86Z", "10.89Z", "10.91Z", "10.92Z",
    "11.01Z", "11.02A", "11.02B", "11.03Z", "11.04Z", "11.05Z", "11.06Z", "11.07A", "11.07B",
    "12.00Z",
    "13.10Z", "13.20Z", "13.30Z", "13.91Z", "13.92Z", "13.93Z", "13.94Z", "13.95Z", "13.96Z", "13.99Z",
    "14.11Z", "14.12Z", "14.13Z", "14.14Z", "14.19Z", "14.20Z", "14.31Z", "14.39Z",
    "15.11Z", "15.12Z", "15.20Z",
    "16.10A", "16.10B", "16.21Z", "16.22Z", "16.23Z", "16.24Z", "16.29Z",
    "17.11Z", "17.12Z", "17.21A", "17.21B", "17.21C", "17.22Z", "17.23Z", "17.24Z", "17.29Z",
    "18.11Z", "18.12Z", "18.13Z", "18.14Z", "18.20Z",
    "19.10Z", "19.20Z",
    "20.11Z", "20.12Z", "20.13A", "20.13B", "20.14Z", "20.15Z", "20.16Z", "20.17Z", "20.20Z",
    "20.30Z", "20.41Z", "20.42Z", "20.51Z", "20.52Z", "20.53Z", "20.59Z", "20.60Z",
    "21.10Z", "21.20Z",
    "22.11Z", "22.19Z", "22.21Z", "22.22Z", "22.23Z", "22.29Z",
    "23.11Z", "23.12Z", "23.13Z", "23.14Z", "23.19Z", "23.20Z", "23.31Z", "23.32Z",
    "23.41Z", "23.42Z", "23.43Z", "23.44Z", "23.49Z", "23.51Z", "23.52Z", "23.61Z",
    "23.62Z", "23.63Z", "23.64Z", "23.65Z", "23.69Z", "23.70Z", "23.91Z", "23.99Z",
    "24.10Z", "24.20Z", "24.31Z", "24.32Z", "24.33Z", "24.34Z", "24.41Z", "24.42Z",
    "24.43Z", "24.44Z", "24.45Z", "24.46Z", "24.51Z", "24.52Z", "24.53Z", "24.54Z",
    "25.11Z", "25.12Z", "25.13Z", "25.21Z", "25.29Z", "25.30Z", "25.40Z", "25.50A",
    "25.50B", "25.61Z", "25.62A", "25.62B", "25.71Z", "25.72Z", "25.73A", "25.73B",
    "25.91Z", "25.92Z", "25.93Z", "25.94Z", "25.99A", "25.99B",
    "26.11Z", "26.12Z", "26.20Z", "26.30Z", "26.40Z", "26.51A", "26.51B", "26.52Z",
    "26.60Z", "26.70A", "26.70B", "26.80Z",
    "27.11Z", "27.12Z", "27.20Z", "27.31Z", "27.32Z", "27.33Z", "27.40Z", "27.51Z",
    "27.52Z", "27.90Z",
    "28.11Z", "28.12Z", "28.13Z", "28.14Z", "28.15Z", "28.21Z", "28.22Z", "28.23Z",
    "28.24Z", "28.25Z", "28.29A", "28.29B", "28.30Z", "28.41Z", "28.49Z", "28.91Z",
    "28.92Z", "28.93Z", "28.94Z", "28.95Z", "28.96Z", "28.99A", "28.99B",
    "29.10Z", "29.20Z", "29.31Z", "29.32Z",
    "30.11Z", "30.12Z", "30.20Z", "30.30Z", "30.40Z", "30.91Z", "30.92Z", "30.99Z",
    "31.01Z", "31.02Z", "31.03Z", "31.09A", "31.09B",
    "32.11Z", "32.12Z", "32.13Z", "32.20Z", "32.30Z", "32.40Z", "32.50A", "32.50B",
    "32.91Z", "32.99Z",
    "33.11Z", "33.12Z", "33.13Z", "33.14Z", "33.15Z", "33.16Z", "33.17Z", "33.19Z", "33.20A", "33.20B"
]



# ============================================================
# FONCTIONS API
# ============================================================

def fetch_page(naf_code, page, retries=5):
    for attempt in range(retries):
        params = {
            "activite_principale":  naf_code,
            "region":               REGION_IDF,
            "categorie_entreprise": CATEGORIE,
            "per_page":             PER_PAGE,
            "page":                 page,
        }
        try:
            r = requests.get(BASE_URL, params=params, timeout=10)
            if r.status_code == 429:
                wait = 2 ** attempt
                print(f"   ⏳ 429 rate limit — attente {wait}s (tentative {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️ Tentative {attempt+1}/{retries} échouée : {e}")
            time.sleep(2)
    print(f"   ❌ Abandon après {retries} tentatives (NAF {naf_code} page {page})")
    return None


def get_total_pages(total_results, per_page):
    max_results = min(total_results, 10000)
    return (max_results + per_page - 1) // per_page


def extract_dirigeants(entreprise):
    rows = []
    siege = entreprise.get('siege', {}) or {}
    info_ent = {
        'siren':          entreprise.get('siren', ''),
        'nom_entreprise': entreprise.get('nom_complet', ''),
        'naf':            entreprise.get('activite_principale', ''),
        'categorie':      entreprise.get('categorie_entreprise', ''),
        'effectif':       entreprise.get('tranche_effectif_salarie', ''),
        'adresse':        siege.get('adresse', ''),
        'code_postal':    siege.get('code_postal', ''),
        'commune':        siege.get('commune', ''),
    }
    for d in entreprise.get('dirigeants', []):
        nom     = d.get('nom', '') or ''
        prenoms = d.get('prenoms', '') or ''
        if not nom.strip() and not prenoms.strip():
            continue
        row = {**info_ent}
        row['dirigeant_nom']       = nom
        row['dirigeant_prenoms']   = prenoms
        row['dirigeant_qualite']   = d.get('qualite', '')
        row['date_prise_de_poste'] = d.get('date_prise_de_poste', '')
        row['date_naissance']      = d.get('date_de_naissance', '')
        row['collecte_le']         = datetime.now().strftime('%Y-%m-%d')
        rows.append(row)
    return rows


def collecter_tous_naf(codes_naf):
    tous = []
    stats = {'entreprises': 0, 'dirigeants': 0, 'sans_dirigeants': 0, 'erreurs': 0}

    print(f"🚀 Collecte — {len(codes_naf)} codes NAF | IDF | {CATEGORIE}")

    for i, naf in enumerate(tqdm(codes_naf, desc="Codes NAF")):
        data = fetch_page(naf, page=1)
        if data is None:
            stats['erreurs'] += 1
            continue

        total    = data.get('total_results', 0)
        nb_pages = get_total_pages(total, PER_PAGE)
        tqdm.write(f"  [{i+1:03d}/{len(codes_naf)}] NAF {naf} → {total} entreprises ({nb_pages} pages)")

        if total == 0:
            continue

        for page in tqdm(range(1, nb_pages + 1), desc=f"  {naf}", leave=False):
            if page > 1:
                data = fetch_page(naf, page=page)
                if data is None:
                    stats['erreurs'] += 1
                    continue

            for ent in data.get('results', []):
                stats['entreprises'] += 1
                dirigs = extract_dirigeants(ent)
                if dirigs:
                    tous.extend(dirigs)
                    stats['dirigeants'] += len(dirigs)
                else:
                    stats['sans_dirigeants'] += 1

            time.sleep(DELAY_BETWEEN_CALLS)

    print(f"\n✅ Collecte terminée !")
    print(f"   Entreprises : {stats['entreprises']} | Dirigeants : {stats['dirigeants']}")
    print(f"   Sans dirigeants : {stats['sans_dirigeants']} | Erreurs API : {stats['erreurs']}")

    return pd.DataFrame(tous)


# ============================================================
# FONCTIONS SNAPSHOT
# ============================================================

def get_dernier_snapshot():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))
    return snapshots[-1] if snapshots else None


def sauvegarder_snapshot(df):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    path = SNAPSHOT_DIR / f"snapshot_{timestamp}.csv"
    df.to_csv(path, index=False, encoding='utf-8-sig')
    print(f"💾 Snapshot sauvegardé : {path}")
    return path


def comparer_snapshots(path_ancien_csv, df_nouveau):
    df_ancien = pd.read_csv(path_ancien_csv, dtype=str).fillna('')
    df_new    = df_nouveau.fillna('').astype(str)

    print(f"📂 Snapshot T   : {len(df_ancien)} lignes")
    print(f"📂 Snapshot T+1 : {len(df_new)} lignes")

    cle_cols = ['siren', 'dirigeant_nom', 'dirigeant_prenoms', 'dirigeant_qualite']

    def make_keys(df):
        return set(df[cle_cols].apply(lambda r: '||'.join(r.values.astype(str)), axis=1))

    cles_ancien = make_keys(df_ancien)
    cles_new    = make_keys(df_new)

    df_ancien['_cle'] = df_ancien[cle_cols].apply(lambda r: '||'.join(r.values.astype(str)), axis=1)
    df_new['_cle']    = df_new[cle_cols].apply(lambda r: '||'.join(r.values.astype(str)), axis=1)

    df_nouveaux = df_new[df_new['_cle'].isin(cles_new - cles_ancien)].drop(columns=['_cle']).copy()
    df_disparus = df_ancien[df_ancien['_cle'].isin(cles_ancien - cles_new)].drop(columns=['_cle']).copy()

    df_nouveaux['statut'] = 'NOUVEAU'
    df_disparus['statut'] = 'DISPARU'

    print(f"\n🎯 Nouveaux dirigeants : {len(df_nouveaux)}")
    print(f"   Dirigeants disparus : {len(df_disparus)}")

    return df_nouveaux, df_disparus


# ============================================================
# GOOGLE SHEETS
# ============================================================

def get_google_sheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise ValueError("Variable d'environnement GOOGLE_CREDENTIALS manquante")

    creds_dict = json.loads(creds_json)
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open(GOOGLE_SHEET_NAME)


def pousser_vers_sheets(df_nouveaux, df_disparus):
    if df_nouveaux.empty and df_disparus.empty:
        print("📊 Aucun changement à pousser dans Google Sheets.")
        return

    sheet = get_google_sheet()
    date_today = datetime.now().strftime('%Y-%m-%d')

    if not df_nouveaux.empty:
        try:
            ws = sheet.worksheet("Nouveaux dirigeants")
        except gspread.WorksheetNotFound:
            ws = sheet.add_worksheet("Nouveaux dirigeants", rows=10000, cols=20)
            ws.append_row(list(df_nouveaux.columns) + ['date_detection'])
        rows = df_nouveaux.copy()
        rows['date_detection'] = date_today
        ws.append_rows(rows.fillna('').values.tolist())
        print(f"✅ {len(df_nouveaux)} nouveaux dirigeants ajoutés dans Google Sheets")

    if not df_disparus.empty:
        try:
            ws = sheet.worksheet("Dirigeants disparus")
        except gspread.WorksheetNotFound:
            ws = sheet.add_worksheet("Dirigeants disparus", rows=10000, cols=20)
            ws.append_row(list(df_disparus.columns) + ['date_detection'])
        rows = df_disparus.copy()
        rows['date_detection'] = date_today
        ws.append_rows(rows.fillna('').values.tolist())
        print(f"✅ {len(df_disparus)} dirigeants disparus ajoutés dans Google Sheets")


# ============================================================
# MAIN
# ============================================================

def main():
    print(f"\n{'='*60}")
    print(f"🤖 Agent Dirigeants IDF — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    df_nouveau = collecter_tous_naf(CODES_NAF)

    path_ancien = get_dernier_snapshot()

    if path_ancien is None:
        print("\n📋 Premier lancement — création du snapshot de référence.")
        sauvegarder_snapshot(df_nouveau)
        print("✅ Snapshot créé. Les changements seront détectés dès demain.")
        return

    print(f"\n🔍 Comparaison avec : {path_ancien.name}")
    df_nouveaux, df_disparus = comparer_snapshots(path_ancien, df_nouveau)

    print("\n📊 Envoi vers Google Sheets...")
    pousser_vers_sheets(df_nouveaux, df_disparus)

    sauvegarder_snapshot(df_nouveau)

    print(f"\n{'='*60}")
    print(f"✅ Terminé — prochain lancement demain à 7h")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
