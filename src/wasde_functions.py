import os
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from config import WASDE_FOLDER, RAW_DATA
import pandas as pd
import os
import re
from urllib.parse import urlparse


# Fetches a list of WASDE report metadata from the USDA API, filtered by a date range. Requires a valid authentication token.
def fetch_wasde_releases(token, start_date="2000-01-01", end_date="2026-01-01"):
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    wasde_identifier = "wasde"
    url = f"https://usda.library.cornell.edu/api/v1/release/findByIdentifier/{wasde_identifier}?latest=false&start_date={start_date}&end_date={end_date}"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

# Downloads .xls report files from the list of releases obtained via the API. Saves them to a local directory, optionally limiting the number of downloads.
def download_release_files(releases, limit=None):
    WASDE_FOLDER.mkdir(parents=True, exist_ok=True)
    downloaded = 0

    for release in releases:
        release_date = release.get("release_datetime", "")[:10]
        files = release.get("files", [])

        for file_url in files:
            # Baixar apenas arquivos .xls
            if file_url.endswith(".xls"):
                filename = os.path.basename(urlparse(file_url).path)
                filename = f"{release_date}_{filename}"
                save_path = WASDE_FOLDER / filename

                if save_path.exists():
                    print(f"Already exists: {filename}, skipping.")
                    continue

                file_resp = requests.get(file_url)
                if file_resp.status_code == 200:
                    with open(save_path, "wb") as f_out:
                        f_out.write(file_resp.content)
                    print(f"Downloaded: {filename}")
                    downloaded += 1
                    if limit and downloaded >= limit:
                        return
                else:
                    print(f"Failed to download {file_url}: Status {file_resp.status_code}")

# Scans a DataFrame to detect the row index where the header (usually starting with "beginning stocks") appears. Used to clean and standardize the structure of USDA tables.
def detect_header_start(df, min_row=3, max_row=15):
    for i in range(min_row, max_row):
        row_values = df.iloc[i].astype(str).str.lower()
        if any('beginning' in val for val in row_values):
            return i
    return 7

# Identifies the last row index in the DataFrame containing a crop year pattern (e.g., "2024/25"). Typically used to separate current and next marketing year data for grains like wheat and corn.
def find_line(df):
    pattern = re.compile(r'\b\d{4}\s*/\s*\d{2}(?:\s*Est\.?)?', re.IGNORECASE)
    last_idx = None
    for idx in range(len(df)):
        row = df.iloc[idx, :].astype(str).str.lower().str.replace('\xa0', ' ').str.replace(r'\s+', ' ', regex=True)
        if row.str.contains(pattern).any():
            last_idx = idx
    return last_idx

# A more flexible version of find_line. It returns a list of all row indices that match a crop year pattern, useful when multiple blocks (e.g., current, next, outlook) exist in the same table (especially for soybeans).
def find_line_v2(df):
    """
    Encontra todos os índices de linhas que contêm datas no formato 'YYYY/YY'.
    Retorna uma lista com os índices encontrados.
    """
    pattern = re.compile(r'\b\d{4}\s*/\s*\d{2}(?:\s*Est\.?)?', re.IGNORECASE)
    match_indices = []
    
    for idx in range(len(df)):
        row = df.iloc[idx, :].astype(str).str.lower().str.replace('\xa0', ' ').str.replace(r'\s+', ' ', regex=True)
        if row.str.contains(pattern).any():
            match_indices.append(idx)
    
    return match_indices

# Standardizes column names by converting them to lowercase, trimming whitespace, and removing duplicates. Helps with consistent downstream processing.
def clean_columns(df):
    df.columns = pd.Index([str(col).strip().lower() for col in df.columns])
    df = df.loc[:, ~df.columns.duplicated()]
    return df

# Simplifies column names by removing suffixes (everything after an underscore), while preserving key columns like country and report_date.
def reset_column_names(df):
    return df.rename(columns={col: col.split('_')[0] if col not in ['country', 'report_date'] else col for col in df.columns})

# Reshapes the DataFrame by extracting data for multiple countries and renaming selected columns to encode metadata such as commodity type and crop stage. Consolidates the output into a single row per report date.
def pivot_df(df, countries, pivot_cols, commodity, crop_stage):
    df_pivot = pd.DataFrame()
    report_date_col = None
    for idx, country in enumerate(countries):
        df_country = df[df['country'] == country].drop(columns=['country']).reset_index(drop=True).head(1)
        if idx == 0:
            report_date_col = df_country['report_date']
        else:
            df_country = df_country.drop(columns=['report_date'], errors='ignore')
        df_country = df_country.rename(columns={col: f"{col.lower()}_{commodity}_{crop_stage}_{country.lower().replace(' ', '')}" for col in pivot_cols})
        df_pivot = pd.concat([df_pivot, df_country], axis=1)
    if report_date_col is not None:
        if 'report_date' in df_pivot.columns:
            df_pivot = df_pivot.drop(columns=['report_date'])
        df_pivot.insert(0, 'report_date', report_date_col.reset_index(drop=True))
    return df_pivot

######################################################################################################
# Commodities
######################################################################################################

# Wheat
def process_wheat(wasde_path):
    try:
        df_wheat_raw = pd.read_excel(wasde_path, sheet_name='Page 18')
        df_wheat_raw2 = pd.read_excel(wasde_path, sheet_name='Page 19')
    except Exception as e:
        print(f"⚠️ Error reading {wasde_path}: {e}")
        return None

    header_idx = detect_header_start(df_wheat_raw)
    df_wheat_raw = df_wheat_raw.iloc[header_idx:]
    header = df_wheat_raw.iloc[0]
    df_wheat_raw = df_wheat_raw[1:]
    df_wheat_raw.columns = header

    filename = os.path.basename(wasde_path)
    report_date = filename.split('_')[0]
    df_wheat_raw['report_date'] = report_date

    broken_line = find_line(df_wheat_raw)

    df_wheat_current = df_wheat_raw.iloc[:broken_line].copy()
    df_wheat_next = df_wheat_raw.iloc[broken_line + 1:].copy()

    df_wheat_current = df_wheat_current[~df_wheat_current.iloc[:, 0].astype(str).str.contains('Est.')].dropna(how='all')
    df_wheat_next = df_wheat_next[~df_wheat_next.iloc[:, 0].astype(str).str.contains('Est.')].dropna(how='all')

    df_wheat_current['crop_stage'] = "current year"
    df_wheat_next['crop_stage'] = "next year"

    df_wheat_raw2 = df_wheat_raw2.iloc[7:]
    header2 = df_wheat_raw2.iloc[0]
    df_wheat_raw2 = df_wheat_raw2[1:]
    df_wheat_raw2.columns = header2
    df_wheat_raw2 = df_wheat_raw2.dropna(axis=1, how='all')
    df_wheat_raw2.iloc[:, 0] = df_wheat_raw2.iloc[:, 0].shift(1)
    df_wheat_raw2 = df_wheat_raw2.dropna(how='all')
    df_wheat_raw2 = df_wheat_raw2.loc[:, ~df_wheat_raw2.columns.isna()]
    df_wheat_current = df_wheat_current.loc[:, ~df_wheat_current.columns.isna()]
    df_wheat_next = df_wheat_next.loc[:, ~df_wheat_next.columns.isna()]
    df_wheat_raw2['report_date'] = report_date

    columns_dict = {df_wheat_raw2.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                    'Imports': 'imports', 'Domestic\nFeed': 'domestic_feed', 'Domestic\nTotal 2/': 'domestic_total',
                    'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    columns_dict2 = {df_wheat_current.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                        'Imports': 'imports', 'Domestic\nFeed': 'domestic_feed', 'Domestic\nTotal 2/': 'domestic_total',
                        'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    columns_dict3 = {df_wheat_next.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                        'Imports': 'imports', 'Domestic\nFeed': 'domestic_feed', 'Domestic\nTotal 2/': 'domestic_total',
                        'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    df_wheat_raw2 = df_wheat_raw2.rename(columns=columns_dict)
    df_wheat_current = df_wheat_current.rename(columns=columns_dict2)
    df_wheat_next = df_wheat_next.rename(columns=columns_dict3)

    df_wheat_raw2['country'] = df_wheat_raw2['country'].astype(str).str.strip()
    df_wheat_current['country'] = df_wheat_current['country'].astype(str).str.strip()
    df_wheat_next['country'] = df_wheat_next['country'].astype(str).str.strip()

    df_wheat_raw2['crop_stage'] = 'outlook year'
    df_wheat_outlook = df_wheat_raw2.dropna(subset=['country'], how='all').reset_index(drop=True)
    df_wheat_outlook = df_wheat_outlook[~df_wheat_outlook['production'].astype(str).str.lower().str.contains('filler', na=False)]
    df_wheat_outlook = df_wheat_outlook[~df_wheat_outlook['country'].astype(str).str.lower().str.contains('nan', na=False)]

    df_wheat = pd.concat([df_wheat_current, df_wheat_next, df_wheat_outlook], ignore_index=True).dropna()
    df_wheat['commodity'] = "wheat"
    df_wheat = df_wheat.iloc[:-1]

    commodity = 'wheat'
    crop_current = 'cy'
    crop_next = 'ny'
    crop_outlook = 'oy'

    df_wheat_current = df_wheat_current.rename(
        columns={col: f"{col}_{commodity}_{crop_current}" for col in df_wheat_current.columns if col not in ['report_date', 'country']}
    )
    df_wheat_next = df_wheat_next.rename(
        columns={col: f"{col}_{commodity}_{crop_next}" for col in df_wheat_next.columns if col not in ['report_date', 'country']}
    )
    df_wheat_outlook = df_wheat_outlook.rename(
        columns={col: f"{col}_{commodity}_{crop_outlook}" for col in df_wheat_outlook.columns if col not in ['report_date', 'country']}
    )

    df_wheat_current = df_wheat_current.dropna().iloc[:-1]
    df_wheat_next = df_wheat_next.dropna().iloc[:-1]
    df_wheat_outlook = df_wheat_outlook.dropna().iloc[:-1]

    df_wheat_current.drop(f'crop_stage_{commodity}_{crop_current}', axis=1, inplace=True)
    df_wheat_next.drop(f'crop_stage_{commodity}_{crop_next}', axis=1, inplace=True)
    df_wheat_outlook.drop(f'crop_stage_{commodity}_{crop_outlook}', axis=1, inplace=True)

    df_wheat_current = reset_column_names(df_wheat_current)
    df_wheat_next = reset_column_names(df_wheat_next)
    df_wheat_outlook = reset_column_names(df_wheat_outlook)

    countries_current = df_wheat_current['country'].unique()
    pivot_current = [col for col in df_wheat_current.columns if col not in ['country', 'report_date']]
    countries_next = df_wheat_next['country'].unique()
    pivot_next = [col for col in df_wheat_next.columns if col not in ['country', 'report_date']]
    countries_outlook = df_wheat_outlook['country'].unique()
    pivot_outlook = [col for col in df_wheat_outlook.columns if col not in ['country', 'report_date']]

    df_wheat_current = pivot_df(df_wheat_current, countries_current, pivot_current, commodity='wheat', crop_stage='cy')
    df_wheat_next = pivot_df(df_wheat_next, countries_next, pivot_next, commodity='wheat', crop_stage='ny')
    df_wheat_outlook = pivot_df(df_wheat_outlook, countries_outlook, pivot_outlook, commodity='wheat', crop_stage='oy')

    df_wheat_current = clean_columns(df_wheat_current)
    df_wheat_next = clean_columns(df_wheat_next)
    df_wheat_outlook = clean_columns(df_wheat_outlook)
    return df_wheat, df_wheat_current, df_wheat_next, df_wheat_outlook

###################################
# Corn

def process_corn(wasde_path):
    try:
        df_corn_raw = pd.read_excel(wasde_path, sheet_name='Page 22')
        df_corn_raw2 = pd.read_excel(wasde_path, sheet_name='Page 23')
    except Exception as e:
        print(f"⚠️ Error reading {wasde_path}: {e}")
        return None

    header_idx = detect_header_start(df_corn_raw)
    df_corn_raw = df_corn_raw.iloc[header_idx:]
    header = df_corn_raw.iloc[0]
    df_corn_raw = df_corn_raw[1:]
    df_corn_raw.columns = header

    filename = os.path.basename(wasde_path)
    report_date = filename.split('_')[0]
    df_corn_raw['report_date'] = report_date

    broken_line = find_line(df_corn_raw)

    df_corn_current = df_corn_raw.iloc[:broken_line].copy()
    df_corn_next = df_corn_raw.iloc[broken_line + 1:].copy()

    df_corn_current = df_corn_current[~df_corn_current.iloc[:, 0].astype(str).str.contains('Est.')].dropna(how='all')
    df_corn_next = df_corn_next[~df_corn_next.iloc[:, 0].astype(str).str.contains('Est.')].dropna(how='all')

    df_corn_current['crop_stage'] = "current year"
    df_corn_next['crop_stage'] = "next year"

    header2 = detect_header_start(df_corn_raw2)
    df_corn_raw2 = df_corn_raw2.iloc[header2:]
    header2 = df_corn_raw2.iloc[0]
    df_corn_raw2 = df_corn_raw2[1:]
    df_corn_raw2.columns = header2

    df_corn_raw2 = df_corn_raw2.dropna(axis=1, how='all')
    df_corn_raw2.iloc[:, 0] = df_corn_raw2.iloc[:, 0].shift(1)
    df_corn_raw2 = df_corn_raw2.dropna(how='all')
    df_corn_raw2 = df_corn_raw2.loc[:, ~df_corn_raw2.columns.isna()]
    df_corn_current = df_corn_current.loc[:, ~df_corn_current.columns.isna()]
    df_corn_next = df_corn_next.loc[:, ~df_corn_next.columns.isna()]
    df_corn_raw2['report_date'] = report_date

    columns_dict = {df_corn_raw2.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                        'Imports': 'imports', 'Domestic\nFeed': 'domestic_feed', 'Domestic\nTotal 2/': 'domestic_total',
                        'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    columns_dict2 = {df_corn_current.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                        'Imports': 'imports', 'Domestic\nFeed': 'domestic_feed', 'Domestic\nTotal 2/': 'domestic_total',
                        'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    columns_dict3 = {df_corn_next.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                        'Imports': 'imports', 'Domestic\nFeed': 'domestic_feed', 'Domestic\nTotal 2/': 'domestic_total',
                        'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    df_corn_outlook = df_corn_raw2.rename(columns=columns_dict)
    df_corn_current = df_corn_current.rename(columns=columns_dict2)
    df_corn_next = df_corn_next.rename(columns=columns_dict3)

    df_corn_outlook['country'] = df_corn_outlook['country'].astype(str).str.strip()
    df_corn_current['country'] = df_corn_current['country'].astype(str).str.strip()
    df_corn_next['country'] = df_corn_next['country'].astype(str).str.strip()

    df_corn_outlook['crop_stage'] = 'outlook year'
    df_corn_current = df_corn_current.dropna(subset=['production', 'exports'], how='all').reset_index(drop=True)
    df_corn_next = df_corn_next.dropna(subset=['production', 'exports'], how='all').reset_index(drop=True)
    df_corn_outlook = df_corn_outlook.dropna(subset=['production', 'exports'], how='all').reset_index(drop=True)
    df_corn_outlook = df_corn_outlook.dropna(subset=['country'], how='all').reset_index(drop=True)
    df_corn_outlook = df_corn_outlook[~df_corn_outlook['production'].astype(str).str.lower().str.contains('filler', na=False)]
    df_corn_outlook = df_corn_outlook[~df_corn_outlook['country'].astype(str).str.lower().str.contains('nan', na=False)]

    df_corn = pd.concat([df_corn_current, df_corn_next, df_corn_outlook], ignore_index=True).dropna()

    df_corn['commodity'] = "corn"
    df_corn = df_corn.iloc[:-1]

    countries_current = df_corn_current['country'].unique()
    pivot_current = [col for col in df_corn_current.columns if col not in ['country', 'report_date']]
    countries_next = df_corn_next['country'].unique()
    pivot_next = [col for col in df_corn_next.columns if col not in ['country', 'report_date']]
    countries_outlook = df_corn_outlook['country'].unique()
    pivot_outlook = [col for col in df_corn_outlook.columns if col not in ['country', 'report_date']]

    df_corn_current = pivot_df(df_corn_current, countries_current, pivot_current, commodity='corn', crop_stage='cy')
    df_corn_next = pivot_df(df_corn_next, countries_next, pivot_next, commodity='corn', crop_stage='ny')
    df_corn_outlook = pivot_df(df_corn_outlook, countries_outlook, pivot_outlook, commodity='corn', crop_stage='oy')

    df_corn_current = clean_columns(df_corn_current)
    df_corn_next = clean_columns(df_corn_next)
    df_corn_outlook = clean_columns(df_corn_outlook)
    return df_corn, df_corn_current, df_corn_next, df_corn_outlook

###################################
# Soybean

def process_soybean(wasde_path):
    try:
        df_soybean_raw = pd.read_excel(wasde_path, sheet_name='Page 28')
    except Exception as e:
        print(f"⚠️ Error reading {wasde_path}: {e}")
        return None
    
    header_idx = detect_header_start(df_soybean_raw)
    df_soybean_raw = df_soybean_raw.iloc[header_idx:]
    header = df_soybean_raw.iloc[0]
    df_soybean_raw = df_soybean_raw[1:]
    df_soybean_raw.columns = header

    filename = os.path.basename(wasde_path)
    report_date = filename.split('_')[0]
    df_soybean_raw['report_date'] = report_date

    broken_line = find_line_v2(df_soybean_raw)

    df_soybean_current = df_soybean_raw.iloc[:broken_line[0]].copy()
    df_soybean_next = df_soybean_raw.iloc[broken_line[0]+1:broken_line[1]].copy()
    df_soybean_outlook = df_soybean_raw.iloc[broken_line[1]+1:].copy()

    df_soybean_current = df_soybean_current[~df_soybean_current.iloc[:, 0].astype(str).str.contains('Est.')].dropna(how='all')
    df_soybean_next = df_soybean_next[~df_soybean_next.iloc[:, 0].astype(str).str.contains('Est.')].dropna(how='all')

    df_soybean_current['crop_stage'] = "current year"
    df_soybean_next['crop_stage'] = "next year"
    df_soybean_outlook['crop_stage'] = "outlook year"

    df_soybean_current = df_soybean_current.dropna(axis=1, how='all')
    df_soybean_next = df_soybean_next.dropna(axis=1, how='all')
    df_soybean_outlook = df_soybean_outlook.dropna(axis=1, how='all')

    df_soybean_outlook.iloc[:, 0] = df_soybean_outlook.iloc[:, 0].shift(1)

    df_soybean_current = df_soybean_current.dropna(subset=['Production', 'Exports'])
    df_soybean_next = df_soybean_next.dropna(subset=['Production', 'Exports'])
    df_soybean_outlook = df_soybean_outlook.dropna(subset=['Production', 'Exports'])

    df_soybean_current = df_soybean_current.loc[:, ~df_soybean_current.columns.isna()]
    df_soybean_next = df_soybean_next.loc[:, ~df_soybean_next.columns.isna()]
    df_soybean_outlook = df_soybean_outlook.loc[:, ~df_soybean_outlook.columns.isna()]

    columns_dict = {df_soybean_current.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                    'Imports': 'imports', 'Domestic\nCrush': 'domestic_crush', 'Domestic\nTotal/': 'domestic_total',
                    'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    columns_dict2 = {df_soybean_next.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                    'Imports': 'imports', 'Domestic\nCrush': 'domestic_crush', 'Domestic\nTotal/': 'domestic_total',
                    'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    columns_dict3 = {df_soybean_outlook.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                    'Imports': 'imports', 'Domestic\nCrush': 'domestic_crush', 'Domestic\nTotal/': 'domestic_total',
                    'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    df_soybean_current = df_soybean_current.rename(columns=columns_dict)
    df_soybean_next = df_soybean_next.rename(columns=columns_dict2)
    df_soybean_outlook = df_soybean_outlook.rename(columns=columns_dict3)

    df_soybean_current['country'] = df_soybean_current['country'].astype(str).str.strip()
    df_soybean_next['country'] = df_soybean_next['country'].astype(str).str.strip()
    df_soybean_outlook['country'] = df_soybean_outlook['country'].astype(str).str.strip()
    df_soybean_outlook = df_soybean_outlook[~df_soybean_outlook['country'].astype(str).str.lower().str.contains('nan', na=False)]
    df_soybean_outlook = df_soybean_outlook[~df_soybean_outlook['country'].astype(str).str.lower().str.contains('None', na=False)]

    df_soybean_outlook = df_soybean_outlook.dropna(subset='country')

    df_soybean = pd.concat([df_soybean_current, df_soybean_next, df_soybean_outlook], ignore_index=True).dropna()
    df_soybean['commodity'] = "soybean"

    commodity = 'soybean'
    crop_current = 'cy'
    crop_next = 'ny'
    crop_outlook = 'oy'

    df_soybean_current = df_soybean_current.rename(
        columns={col: f"{col}_{commodity}_{crop_current}" for col in df_soybean_current.columns if col not in ['report_date', 'country']}
    )
    df_soybean_next = df_soybean_next.rename(
        columns={col: f"{col}_{commodity}_{crop_next}" for col in df_soybean_next.columns if col not in ['report_date', 'country']}
    )
    df_soybean_outlook = df_soybean_outlook.rename(
        columns={col: f"{col}_{commodity}_{crop_outlook}" for col in df_soybean_outlook.columns if col not in ['report_date', 'country']}
    )

    df_soybean_current = df_soybean_current.dropna().iloc[:-1]
    df_soybean_next = df_soybean_next.dropna().iloc[:-1]
    df_soybean_outlook = df_soybean_outlook.dropna().iloc[:-1]

    df_soybean_current.drop(f'crop_stage_{commodity}_{crop_current}', axis=1, inplace=True)
    df_soybean_next.drop(f'crop_stage_{commodity}_{crop_next}', axis=1, inplace=True)
    df_soybean_outlook.drop(f'crop_stage_{commodity}_{crop_outlook}', axis=1, inplace=True)

    df_soybean_current = reset_column_names(df_soybean_current)
    df_soybean_next = reset_column_names(df_soybean_next)
    df_soybean_outlook = reset_column_names(df_soybean_outlook)

    countries_current = df_soybean_current['country'].unique()
    pivot_current = [col for col in df_soybean_current.columns if col not in ['country', 'report_date']]
    countries_next = df_soybean_next['country'].unique()
    pivot_next = [col for col in df_soybean_next.columns if col not in ['country', 'report_date']]
    countries_outlook = df_soybean_outlook['country'].unique()
    pivot_outlook = [col for col in df_soybean_outlook.columns if col not in ['country', 'report_date']]

    df_soybean_current = pivot_df(df_soybean_current, countries_current, pivot_current, commodity='soybean', crop_stage='cy')
    df_soybean_next = pivot_df(df_soybean_next, countries_next, pivot_next, commodity='soybean', crop_stage='ny')
    df_soybean_outlook = pivot_df(df_soybean_outlook, countries_outlook, pivot_outlook, commodity='soybean', crop_stage='oy')

    df_soybean_current = clean_columns(df_soybean_current)
    df_soybean_next = clean_columns(df_soybean_next)
    df_soybean_outlook = clean_columns(df_soybean_outlook)
    return df_soybean, df_soybean_current, df_soybean_next, df_soybean_outlook

###################################
# Soybean oil

def process_soybean_oil(wasde_path):
    try:
        df_soybean_oil_raw = pd.read_excel(wasde_path, sheet_name='Page 30')
    except Exception as e:
        print(f"⚠️ Error reading {wasde_path}: {e}")
        return None
    
    header_idx = detect_header_start(df_soybean_oil_raw)
    df_soybean_oil_raw = df_soybean_oil_raw.iloc[header_idx:]
    header = df_soybean_oil_raw.iloc[0]
    df_soybean_oil_raw = df_soybean_oil_raw[1:]
    df_soybean_oil_raw.columns = header

    filename = os.path.basename(wasde_path)
    report_date = filename.split('_')[0]
    df_soybean_oil_raw['report_date'] = report_date

    broken_line = find_line_v2(df_soybean_oil_raw)

    df_soybean_oil_current = df_soybean_oil_raw.iloc[:broken_line[0]].copy()
    df_soybean_oil_next = df_soybean_oil_raw.iloc[broken_line[0]+1:broken_line[1]].copy()
    df_soybean_oil_outlook = df_soybean_oil_raw.iloc[broken_line[1]+1:].copy()

    df_soybean_oil_current = df_soybean_oil_current[~df_soybean_oil_current.iloc[:, 0].astype(str).str.contains('Est.')].dropna(how='all')
    df_soybean_oil_next = df_soybean_oil_next[~df_soybean_oil_next.iloc[:, 0].astype(str).str.contains('Est.')].dropna(how='all')

    df_soybean_oil_current['crop_stage'] = "current year"
    df_soybean_oil_next['crop_stage'] = "next year"
    df_soybean_oil_outlook['crop_stage'] = "outlook year"

    df_soybean_oil_current = df_soybean_oil_current.dropna(axis=1, how='all')
    df_soybean_oil_next = df_soybean_oil_next.dropna(axis=1, how='all')
    df_soybean_oil_outlook = df_soybean_oil_outlook.dropna(axis=1, how='all')

    df_soybean_oil_outlook.iloc[:, 0] = df_soybean_oil_outlook.iloc[:, 0].shift(1)

    df_soybean_oil_current = df_soybean_oil_current.dropna(subset=['Production', 'Exports'])
    df_soybean_oil_next = df_soybean_oil_next.dropna(subset=['Production', 'Exports'])
    df_soybean_oil_outlook = df_soybean_oil_outlook.dropna(subset=['Production', 'Exports'])

    df_soybean_oil_current = df_soybean_oil_current.loc[:, ~df_soybean_oil_current.columns.isna()]
    df_soybean_oil_next = df_soybean_oil_next.loc[:, ~df_soybean_oil_next.columns.isna()]
    df_soybean_oil_outlook = df_soybean_oil_outlook.loc[:, ~df_soybean_oil_outlook.columns.isna()]

    columns_dict = {df_soybean_oil_current.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                    'Imports': 'imports', 'Domestic\nCrush': 'domestic_crush', 'Domestic\nTotal/': 'domestic_total',
                    'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    columns_dict2 = {df_soybean_oil_next.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                    'Imports': 'imports', 'Domestic\nCrush': 'domestic_crush', 'Domestic\nTotal/': 'domestic_total',
                    'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    columns_dict3 = {df_soybean_oil_outlook.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                    'Imports': 'imports', 'Domestic\nCrush': 'domestic_crush', 'Domestic\nTotal/': 'domestic_total',
                    'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    df_soybean_oil_current = df_soybean_oil_current.rename(columns=columns_dict)
    df_soybean_oil_next = df_soybean_oil_next.rename(columns=columns_dict2)
    df_soybean_oil_outlook = df_soybean_oil_outlook.rename(columns=columns_dict3)

    df_soybean_oil_current['country'] = df_soybean_oil_current['country'].astype(str).str.strip()
    df_soybean_oil_next['country'] = df_soybean_oil_next['country'].astype(str).str.strip()
    df_soybean_oil_outlook['country'] = df_soybean_oil_outlook['country'].astype(str).str.strip()
    df_soybean_oil_outlook = df_soybean_oil_outlook[~df_soybean_oil_outlook['country'].astype(str).str.lower().str.contains('nan', na=False)]
    df_soybean_oil_outlook = df_soybean_oil_outlook[~df_soybean_oil_outlook['country'].astype(str).str.lower().str.contains('None', na=False)]

    df_soybean_oil_outlook = df_soybean_oil_outlook.dropna(subset='country')

    df_soybean_oil = pd.concat([df_soybean_oil_current, df_soybean_oil_next, df_soybean_oil_outlook], ignore_index=True).dropna()
    df_soybean_oil['commodity'] = "soybean_oil"

    commodity = 'soybean_oil'
    crop_current = 'cy'
    crop_next = 'ny'
    crop_outlook = 'oy'

    df_soybean_oil_current = df_soybean_oil_current.rename(
        columns={col: f"{col}_{commodity}_{crop_current}" for col in df_soybean_oil_current.columns if col not in ['report_date', 'country']}
    )
    df_soybean_oil_next = df_soybean_oil_next.rename(
        columns={col: f"{col}_{commodity}_{crop_next}" for col in df_soybean_oil_next.columns if col not in ['report_date', 'country']}
    )
    df_soybean_oil_outlook = df_soybean_oil_outlook.rename(
        columns={col: f"{col}_{commodity}_{crop_outlook}" for col in df_soybean_oil_outlook.columns if col not in ['report_date', 'country']}
    )

    df_soybean_oil_current = df_soybean_oil_current.dropna().iloc[:-1]
    df_soybean_oil_next = df_soybean_oil_next.dropna().iloc[:-1]
    df_soybean_oil_outlook = df_soybean_oil_outlook.dropna().iloc[:-1]

    df_soybean_oil_current.drop(f'crop_stage_{commodity}_{crop_current}', axis=1, inplace=True)
    df_soybean_oil_next.drop(f'crop_stage_{commodity}_{crop_next}', axis=1, inplace=True)
    df_soybean_oil_outlook.drop(f'crop_stage_{commodity}_{crop_outlook}', axis=1, inplace=True)

    df_soybean_oil_current = reset_column_names(df_soybean_oil_current)
    df_soybean_oil_next = reset_column_names(df_soybean_oil_next)
    df_soybean_oil_outlook = reset_column_names(df_soybean_oil_outlook)

    countries_current = df_soybean_oil_current['country'].unique()
    pivot_current = [col for col in df_soybean_oil_current.columns if col not in ['country', 'report_date']]
    countries_next = df_soybean_oil_next['country'].unique()
    pivot_next = [col for col in df_soybean_oil_next.columns if col not in ['country', 'report_date']]
    countries_outlook = df_soybean_oil_outlook['country'].unique()
    pivot_outlook = [col for col in df_soybean_oil_outlook.columns if col not in ['country', 'report_date']]

    df_soybean_oil_current = pivot_df(df_soybean_oil_current, countries_current, pivot_current, commodity='soybean_oil', crop_stage='cy')
    df_soybean_oil_next = pivot_df(df_soybean_oil_next, countries_next, pivot_next, commodity='soybean_oil', crop_stage='ny')
    df_soybean_oil_outlook = pivot_df(df_soybean_oil_outlook, countries_outlook, pivot_outlook, commodity='soybean_oil', crop_stage='oy')

    df_soybean_oil_current = clean_columns(df_soybean_oil_current)
    df_soybean_oil_next = clean_columns(df_soybean_oil_next)
    df_soybean_oil_outlook = clean_columns(df_soybean_oil_outlook)
    return df_soybean_oil, df_soybean_oil_current, df_soybean_oil_next, df_soybean_oil_outlook


###################################
# Soybean meal

def process_soybean_meal(wasde_path):
    try:
        df_soybean_meal_raw = pd.read_excel(wasde_path, sheet_name='Page 29')
    except Exception as e:
        print(f"⚠️ Error reading {wasde_path}: {e}")
        return None
    
    header_idx = detect_header_start(df_soybean_meal_raw)
    df_soybean_meal_raw = df_soybean_meal_raw.iloc[header_idx:]
    header = df_soybean_meal_raw.iloc[0]
    df_soybean_meal_raw = df_soybean_meal_raw[1:]
    df_soybean_meal_raw.columns = header

    filename = os.path.basename(wasde_path)
    report_date = filename.split('_')[0]
    df_soybean_meal_raw['report_date'] = report_date

    broken_line = find_line_v2(df_soybean_meal_raw)

    df_soybean_meal_current = df_soybean_meal_raw.iloc[:broken_line[0]].copy()
    df_soybean_meal_next = df_soybean_meal_raw.iloc[broken_line[0]+1:broken_line[1]].copy()
    df_soybean_meal_outlook = df_soybean_meal_raw.iloc[broken_line[1]+1:].copy()

    df_soybean_meal_current = df_soybean_meal_current[~df_soybean_meal_current.iloc[:, 0].astype(str).str.contains('Est.')].dropna(how='all')
    df_soybean_meal_next = df_soybean_meal_next[~df_soybean_meal_next.iloc[:, 0].astype(str).str.contains('Est.')].dropna(how='all')

    df_soybean_meal_current['crop_stage'] = "current year"
    df_soybean_meal_next['crop_stage'] = "next year"
    df_soybean_meal_outlook['crop_stage'] = "outlook year"

    df_soybean_meal_current = df_soybean_meal_current.dropna(axis=1, how='all')
    df_soybean_meal_next = df_soybean_meal_next.dropna(axis=1, how='all')
    df_soybean_meal_outlook = df_soybean_meal_outlook.dropna(axis=1, how='all')

    df_soybean_meal_outlook.iloc[:, 0] = df_soybean_meal_outlook.iloc[:, 0].shift(1)

    df_soybean_meal_current = df_soybean_meal_current.dropna(subset=['Production', 'Exports'])
    df_soybean_meal_next = df_soybean_meal_next.dropna(subset=['Production', 'Exports'])
    df_soybean_meal_outlook = df_soybean_meal_outlook.dropna(subset=['Production', 'Exports'])

    df_soybean_meal_current = df_soybean_meal_current.loc[:, ~df_soybean_meal_current.columns.isna()]
    df_soybean_meal_next = df_soybean_meal_next.loc[:, ~df_soybean_meal_next.columns.isna()]
    df_soybean_meal_outlook = df_soybean_meal_outlook.loc[:, ~df_soybean_meal_outlook.columns.isna()]

    columns_dict = {df_soybean_meal_current.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                    'Imports': 'imports', 'Domestic\nCrush': 'domestic_crush', 'Domestic\nTotal/': 'domestic_total',
                    'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    columns_dict2 = {df_soybean_meal_next.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                    'Imports': 'imports', 'Domestic\nCrush': 'domestic_crush', 'Domestic\nTotal/': 'domestic_total',
                    'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    columns_dict3 = {df_soybean_meal_outlook.columns[0]: 'country', 'Beginning\nStocks': 'beginning_stocks', 'Production': 'production',
                    'Imports': 'imports', 'Domestic\nCrush': 'domestic_crush', 'Domestic\nTotal/': 'domestic_total',
                    'Exports': 'exports', 'Ending\nStocks': 'ending_stocks', 'Domestic\nFeed 2/': 'domestic_feed'}

    df_soybean_meal_current = df_soybean_meal_current.rename(columns=columns_dict)
    df_soybean_meal_next = df_soybean_meal_next.rename(columns=columns_dict2)
    df_soybean_meal_outlook = df_soybean_meal_outlook.rename(columns=columns_dict3)

    df_soybean_meal_current['country'] = df_soybean_meal_current['country'].astype(str).str.strip()
    df_soybean_meal_next['country'] = df_soybean_meal_next['country'].astype(str).str.strip()
    df_soybean_meal_outlook['country'] = df_soybean_meal_outlook['country'].astype(str).str.strip()
    df_soybean_meal_outlook = df_soybean_meal_outlook[~df_soybean_meal_outlook['country'].astype(str).str.lower().str.contains('nan', na=False)]
    df_soybean_meal_outlook = df_soybean_meal_outlook[~df_soybean_meal_outlook['country'].astype(str).str.lower().str.contains('None', na=False)]

    df_soybean_meal_outlook = df_soybean_meal_outlook.dropna(subset='country')

    df_soybean_meal = pd.concat([df_soybean_meal_current, df_soybean_meal_next, df_soybean_meal_outlook], ignore_index=True).dropna()
    df_soybean_meal['commodity'] = "soybean_meal"

    commodity = 'soybean_meal'
    crop_current = 'cy'
    crop_next = 'ny'
    crop_outlook = 'oy'

    df_soybean_meal_current = df_soybean_meal_current.rename(
        columns={col: f"{col}_{commodity}_{crop_current}" for col in df_soybean_meal_current.columns if col not in ['report_date', 'country']}
    )
    df_soybean_meal_next = df_soybean_meal_next.rename(
        columns={col: f"{col}_{commodity}_{crop_next}" for col in df_soybean_meal_next.columns if col not in ['report_date', 'country']}
    )
    df_soybean_meal_outlook = df_soybean_meal_outlook.rename(
        columns={col: f"{col}_{commodity}_{crop_outlook}" for col in df_soybean_meal_outlook.columns if col not in ['report_date', 'country']}
    )

    df_soybean_meal_current = df_soybean_meal_current.dropna().iloc[:-1]
    df_soybean_meal_next = df_soybean_meal_next.dropna().iloc[:-1]
    df_soybean_meal_outlook = df_soybean_meal_outlook.dropna().iloc[:-1]

    df_soybean_meal_current.drop(f'crop_stage_{commodity}_{crop_current}', axis=1, inplace=True)
    df_soybean_meal_next.drop(f'crop_stage_{commodity}_{crop_next}', axis=1, inplace=True)
    df_soybean_meal_outlook.drop(f'crop_stage_{commodity}_{crop_outlook}', axis=1, inplace=True)

    df_soybean_meal_current = reset_column_names(df_soybean_meal_current)
    df_soybean_meal_next = reset_column_names(df_soybean_meal_next)
    df_soybean_meal_outlook = reset_column_names(df_soybean_meal_outlook)

    countries_current = df_soybean_meal_current['country'].unique()
    pivot_current = [col for col in df_soybean_meal_current.columns if col not in ['country', 'report_date']]
    countries_next = df_soybean_meal_next['country'].unique()
    pivot_next = [col for col in df_soybean_meal_next.columns if col not in ['country', 'report_date']]
    countries_outlook = df_soybean_meal_outlook['country'].unique()
    pivot_outlook = [col for col in df_soybean_meal_outlook.columns if col not in ['country', 'report_date']]

    df_soybean_meal_current = pivot_df(df_soybean_meal_current, countries_current, pivot_current, commodity='soybean_meal', crop_stage='cy')
    df_soybean_meal_next = pivot_df(df_soybean_meal_next, countries_next, pivot_next, commodity='soybean_meal', crop_stage='ny')
    df_soybean_meal_outlook = pivot_df(df_soybean_meal_outlook, countries_outlook, pivot_outlook, commodity='soybean_meal', crop_stage='oy')

    df_soybean_meal_current = clean_columns(df_soybean_meal_current)
    df_soybean_meal_next = clean_columns(df_soybean_meal_next)
    df_soybean_meal_outlook = clean_columns(df_soybean_meal_outlook)
    return df_soybean_meal, df_soybean_meal_current, df_soybean_meal_next, df_soybean_meal_outlook