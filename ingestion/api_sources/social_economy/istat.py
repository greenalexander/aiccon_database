import pandas as pd
import requests
import io
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from ingestion.loaders.base_loader import BaseLoader

class IstatSocialEconomyLoader(BaseLoader):
    def __init__(self):
        super().__init__(domain="social_economy")
        self.base_url = self.config['istat']['base_url']
        self.datasets_to_fetch = self.config['istat']['social_economy']

    # RELIABILITY TWEAK: 
    # - Retry up to 5 times. 
    # - Wait increases exponentially: 10s, 20s, 40s, 80s... 
    # - Only retry on connection/timeout errors, not 404s.
    @retry(
        stop=stop_after_attempt(5), 
        wait=wait_exponential(multiplier=2, min=10, max=120),
        retry=retry_if_exception_type((requests.exceptions.RequestException)),
        reraise=True
    )
    def fetch_dataset(self, dataset_code):
        # We request CSV format. ISTAT's REST syntax: /data/{resourceID}/{key}
        url = f"{self.base_url}/data/{dataset_code}/all"
        params = {"format": "csv"}
        
        print(f"Requesting ISTAT dataset: {dataset_code} (This may take a few minutes)...")
        
        # RELIABILITY TWEAK: 300 second timeout
        response = requests.get(url, params=params, timeout=300)
        
        # Check for specific ISTAT error codes
        if response.status_code == 200:
            # Check if the response is actually a CSV and not an HTML error page
            if "text/csv" in response.headers.get("Content-Type", "") or response.text.startswith("DATAFLOW"):
                df = pd.read_csv(io.StringIO(response.text), low_memory=False)
                return df
            else:
                raise requests.exceptions.RequestException("Received non-CSV response from ISTAT")
        elif response.status_code == 404:
            print(f"Data not found (404) for {dataset_code}. Skipping.")
            return None
        else:
            print(f"Server returned status {response.status_code}. Retrying...")
            response.raise_for_status()

    def fetch_all(self):
        for entry in self.datasets_to_fetch:
            code = entry['dataset']
            label = entry['label']
            
            print(f"\n--- Starting: {label} ---")
            try:
                df = self.fetch_dataset(code)
                if df is not None:
                    # Keep all raw data for now; harmonisation happens in Phase 2
                    self.save_to_raw(df, f"istat_{code.lower()}")
                    print(f"Successfully saved {len(df)} rows.")
                
                # Polite gap between datasets
                time.sleep(5) 
                
            except Exception as e:
                print(f"!!! Final failure for {code} after multiple retries: {e}")

if __name__ == "__main__":
    loader = IstatSocialEconomyLoader()
    loader.fetch_all()