import io
import time

import pandas as pd
import requests
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ingestion.loaders.base_loader import save_raw_parquet

# ── CONFIG ────────────────────────────────────────────────────────────────────

with open("config/settings.yaml", "r") as f:
    config = yaml.safe_load(f)

BASE_URL = config["istat"]["base_url"]
DATASETS = config["istat"]["social_economy"]


# ── FETCH ─────────────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True,
)
def fetch_dataset(dataset_code: str) -> pd.DataFrame | None:
    """
    Fetch a single ISTAT dataset by code and return it as a DataFrame.
    Retries up to 5 times on connection/timeout errors (10s, 20s, 40s...).
    Returns None if the dataset is not found (404).
    """
    url = f"{BASE_URL}/data/{dataset_code}/all"
    params = {"format": "csv"}

    print(f"Requesting ISTAT dataset: {dataset_code} (this may take a few minutes)...")

    response = requests.get(url, params=params, timeout=300)

    if response.status_code == 200:
        if "text/csv" in response.headers.get("Content-Type", "") or response.text.startswith("DATAFLOW"):
            return pd.read_csv(io.StringIO(response.text), low_memory=False)
        else:
            raise requests.exceptions.RequestException("Received non-CSV response from ISTAT")
    elif response.status_code == 404:
        print(f"Data not found (404) for {dataset_code}. Skipping.")
        return None
    else:
        print(f"Server returned status {response.status_code}. Retrying...")
        response.raise_for_status()


def fetch_all() -> None:
    """Fetch all ISTAT social economy datasets defined in settings.yaml."""
    for entry in DATASETS:
        code = entry["dataset"]
        label = entry["label"]

        print(f"\n--- Starting: {label} ---")
        try:
            df = fetch_dataset(code)
            if df is not None:
                save_raw_parquet(df, domain="social_economy", filename=f"istat_{code.lower()}")
                print(f"Successfully saved {len(df)} rows.")

            time.sleep(5)  # polite gap between requests

        except Exception as e:
            print(f"!!! Final failure for {code} after multiple retries: {e}")


if __name__ == "__main__":
    fetch_all()