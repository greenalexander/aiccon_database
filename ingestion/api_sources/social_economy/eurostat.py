import pandas as pd
import requests
import io
from ingestion.loaders.base_loader import BaseLoader

class EurostatSocialEconomyLoader(BaseLoader):
    def __init__(self):
        # This tells the BaseLoader to use the 'social_economy' subfolder
        super().__init__(domain="social_economy")
        self.base_url = self.config['eurostat']['base_url']

    def fetch_employment_data(self):
        """
        Fetches employment data (thousand persons) for NACE sectors:
        - Q: Human health and social work activities
        - S94: Activities of membership organisations (subset of S)
        """
        dataset_code = "nama_10r_3empers"
        
        # Define API Parameters
        # We filter for NACE 'Q' and 'S' (Total Services) to be safe, 
        # and 'TOTAL' for comparison.
        params = {
            "format": "SDMX-CSV",
            "lang": "en",
            "nace_r2": "Q;S;TOTAL", # Sectors of interest
            "unit": "THS_PER",      # Thousand persons
            "wstatus": "EMP",       # Employed persons
        }

        # Build the URL
        url = f"{self.base_url}/{dataset_code}"
        
        print(f"Fetching Eurostat data from: {url}")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            # Read the CSV data directly into a Dataframe
            df = pd.read_csv(io.StringIO(response.text))
            
            # Apply your settings.yaml filters for NUTS levels
            # NUTS0 is 2 characters (e.g., IT), NUTS2 is 4 characters (e.g., ITC4)
            nuts_levels = self.config['geography']['eurostat_nuts_levels']
            
            def get_nuts_level(geo_code):
                if len(geo_code) == 2: return 0
                if len(geo_code) == 3: return 1
                if len(geo_code) == 4: return 2
                return 3

            df['nuts_level'] = df['geo'].apply(get_nuts_level)
            df = df[df['nuts_level'].isin(nuts_levels)]

            # Save using the BaseLoader method
            self.save_to_raw(df, "eurostat_employment_nace")
            return df
        else:
            print(f"Error fetching Eurostat data: {response.status_code}")
            return None

if __name__ == "__main__":
    loader = EurostatSocialEconomyLoader()
    loader.fetch_employment_data()