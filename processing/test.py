import pandas as pd

# --- 1. Load the Data ---
df = pd.read_parquet(r'C:\Users\green\AICCON\AICCON - Documenti\data_alex\aiccon-data\processed\social_economy\social_economy_9if3f08r.parquet')
# Try both common separators for your mapping file
try:
    nuts_ref = pd.read_csv('processing/mappings/nuts_istat.csv', sep=';')
    if 'nuts_code' not in nuts_ref.columns:
        nuts_ref = pd.read_csv('processing/mappings/nuts_istat.csv', sep=';')
except:
    nuts_ref = pd.read_csv('processing/mappings/nuts_istat.csv', sep=',')

it_unmatched = [c for c in unmatched if str(c).startswith('IT')]
print(f"Missing Italian Codes: {it_unmatched}")

print("--- GEOGRAPHIC MATCHING CHECK ---")
# Find codes in the data that are NOT in your nuts_istat mapping
data_codes = set(df['nuts_code'].unique())
mapping_codes = set(nuts_ref['nuts_code'].dropna().unique())
# Also check istat_code column since some NUTS 1 use that
mapping_codes.update(nuts_ref['istat_code'].dropna().unique())

unmatched = data_codes - mapping_codes
if unmatched:
    print(f"Found {len(unmatched)} unmatched codes: {unmatched}")
else:
    print("All geographic codes successfully matched!")

print("\n--- LEGAL FORM CHECK ---")
# Since your unified column was empty, let's see what the RAW codes were
if 'legal_form' in df.columns:
    forms = df['legal_form'].unique()
    print(f"Raw legal forms found in data: {forms}")
else:
    print("Column 'legal_form' not found. Check your ISTAT loader output.")

print("\n--- DATA DENSITY MAP (Indicator by NUTS Level) ---")
density = df.groupby(['indicator_code', 'nuts_level']).size().unstack(fill_value=0)
print(density)

print(df.groupby(['indicator_code', 'nuts_level']).size().unstack(fill_value=0).to_string())