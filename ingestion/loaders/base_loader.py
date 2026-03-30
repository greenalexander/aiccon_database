import pandas as pd
import requests
import io
import time
from ingestion.loaders.base_loader import BaseLoader

class EurostatSocialEconomyLoader(BaseLoader):
    def __init__(self):
        super().__init__(domain="social_economy")
        self.base_url = self.config['eurostat']['base_url']
        # Get the list of datasets specifically for this domain from settings.yaml
        self.datasets_to_fetch = self.config['eurostat']['social_economy']

    def fetch_all(self):
        """Loops through all datasets defined in settings.yaml for this domain."""
        for entry in self.datasets_to_fetch:
            code = entry['dataset']
            label = entry['label']
            filters = entry.get('filters', {})
            
            print(f"\n--- Processing Eurostat Dataset: {label} ({code}) ---")
            self.fetch_dataset(code, filters)
            # Small sleep to be polite to the API
            time.sleep(1)

    def fetch_dataset(self, dataset_code, filters):
        """Fetches a specific dataset based on its code and YAML filters."""
        # Prepare API Parameters
        params = {
            "format": "SDMX-CSV",
            "lang": "en",
        }
        
        # Add filters from settings.yaml to the API call
        # Note: API expects multi-values separated by semicolon (e.g., Q;P;S94)
        for key, value in filters.items():
            if isinstance(value, list):
                params[key] = ";".join(value)
            else:
                params[key] = value

        url = f"{self.base_url}/{dataset_code}"
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status() # Raise error if download fails
            
            df = pd.read_csv(io.StringIO(response.text))
            
            # Filter by NUTS level as per your geography settings
            # We use the 'geo' column provided by Eurostat
            nuts_levels = self.config['geography']['eurostat_nuts_levels']
            
            def get_nuts_level(geo_code):
                if len(str(geo_code)) == 2: return 0
                if len(str(geo_code)) == 3: return 1
                if len(str(geo_code)) == 4: return 2
                return 3

            df['nuts_level'] = df['geo'].apply(get_nuts_level)
            df = df[df['nuts_level'].isin(nuts_levels)]

            # Save file using the dataset code as the filename
            self.save_to_raw(df, f"eurostat_{dataset_code}")
            print(f"Saved {len(df)} rows for {dataset_code}")

        except Exception as e:
            print(f"Error fetching {dataset_code}: {e}")

if __name__ == "__main__":
    loader = EurostatSocialEconomyLoader()
    loader.fetch_all()