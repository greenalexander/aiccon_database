import pandas as pd
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv

class NutsMapper:
    def __init__(self):
        load_dotenv()
        with open("settings.yaml", "r") as f:
            self.config = yaml.safe_load(f)
            
        self.root_path = Path(os.getenv("SHAREPOINT_ROOT", "."))
        
        # Load your mapping file
        mapping_path = Path("processing/mappings/nuts_istat.csv")
        self.mapping_df = pd.read_csv(mapping_path, dtype={'istat_code': str})
        
        # Create lookup dictionaries for speed
        # Maps ISTAT code -> NUTS code
        self.code_map = self.mapping_df.set_index('istat_code')['nuts_code'].to_dict()
        # Maps ISTAT code -> NUTS Level
        self.level_map = self.mapping_df.set_index('istat_code')['nuts_level'].to_dict()

    def map_istat_dataframe(self, df: pd.DataFrame, istat_col: str):
        """
        Takes a dataframe and the name of the column containing ISTAT codes.
        Returns the dataframe with 'nuts_code' and 'nuts_level' added.
        """
        # Ensure the ISTAT column is string and padded (e.g., '1' becomes '01')
        # ISTAT province codes are 3 digits, regions are 2.
        df[istat_col] = df[istat_col].astype(str).str.zfill(2)
        
        # Apply the mapping
        df['nuts_code'] = df[istat_col].map(self.code_map)
        df['nuts_level'] = df[istat_col].map(self.level_map)
        
        # Special case: If ISTAT uses 'IT' or 'ITA' for national
        df.loc[df[istat_col].isin(['IT', 'ITA', '000']), 'nuts_code'] = 'IT'
        df.loc[df[istat_col].isin(['IT', 'ITA', '000']), 'nuts_level'] = 0
        
        return df

    def get_geo_metadata(self, nuts_code: str):
        """Returns a dictionary with full names for a given NUTS code."""
        match = self.mapping_df[self.mapping_df['nuts_code'] == nuts_code]
        if not match.empty:
            return match.iloc[0].to_dict()
        return None

# Simple test logic
if __name__ == "__main__":
    mapper = NutsMapper()
    # Test with Bologna code
    print(f"Mapping code 037 (Bologna): {mapper.code_map.get('037')}")