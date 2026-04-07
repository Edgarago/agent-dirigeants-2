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
    "10.11Z","10.12Z","10.13A","10.13B","10.20Z","10.31Z","10.32Z",
    "10.39A","10.39B","10.41A","10.41B","10.42Z","10.51A","10.51B",
    "10.51C","10.51D","10.52Z","10.61A","10.61B","10.62Z","10.71A",
    "10.71B","10.71C","10.71D","10.72Z","10.73Z","10.81Z","10.82Z",
    "10.83Z","10.84Z","10.85Z","10.86Z","10.89Z","10.91Z","10.92Z",
    "11.01Z","11.02A","11.02B","11.03Z","11.04Z","11.05Z",
    "11.06Z","11.07A","11.07B",
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
