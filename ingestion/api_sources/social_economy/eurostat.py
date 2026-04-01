import itertools

import pandas as pd
import yaml

from ingestion.loaders.base_loader import fetch_json, save_raw_parquet

# ── CONFIG ────────────────────────────────────────────────────────────────────

with open("config/settings.yaml", "r") as f:
    config = yaml.safe_load(f)

BASE_URL = config["eurostat"]["base_url"]


# ── FETCHERS ──────────────────────────────────────────────────────────────────

def fetch_employment_data() -> pd.DataFrame | None:
    """
    Fetches employment data (thousand persons) for NACE sector O-Q
    (Public administration, education, health and social work) at NUTS2.

    The Eurostat Statistics API only returns JSON-stat format, so we unpack
    the dimension/value structure into a flat DataFrame before saving.
    """
    dataset_code = "nama_10r_3empers"
    params = {
        "format": "JSON",
        "lang": "EN",
        "nace_r2": "O-Q",
        "unit": "THS_PER",
        "geoLevel": "nuts2",
    }

    url = f"{BASE_URL}/{dataset_code}"
    print(f"Fetching Eurostat data from: {url}")

    data = fetch_json(url, params=params)

    # ── Unpack JSON-stat format ───────────────────────────────────────────────
    # Eurostat returns a JSON-stat object: a dict of dimensions each with a
    # category index, plus a flat "value" dict keyed by position index.
    dims = data["dimension"]
    values = data["value"]

    dim_keys = list(dims.keys())
    dim_values = [list(dims[d]["category"]["index"].keys()) for d in dim_keys]

    # -- DEBUG SECTION --
    print("Dimensions found:", dims.keys())
    print("Number of values in response:", len(values))
    print("First few value keys:", list(values.keys())[:5])
    # --------------------

    rows = []
    for idx, combo in enumerate(itertools.product(*dim_values)):
        row = dict(zip(dim_keys, combo))
        row["value"] = values.get(str(idx))  # None where data is missing
        rows.append(row)

    df = pd.DataFrame(rows)

    # Normalise column names to lowercase
    df.columns = [c.lower() for c in df.columns]

    print(f"Fetched {len(df)} rows.")
    save_raw_parquet(df, domain="social_economy", filename="eurostat_employment_nace")
    return df


if __name__ == "__main__":
    fetch_employment_data()