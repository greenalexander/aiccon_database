import pandas as pd
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv

class LegalFormMapper:
    def __init__(self):
        load_dotenv()
        with open("settings.yaml", "r") as f:
            self.config = yaml.safe_load(f)
            
        # Path to your mapping file
        self.mapping_path = Path("processing/mappings/legal_form_map.csv")
        self.mapping_df = pd.read_csv(self.mapping_path)
        
        # We create a nested dictionary for fast lookup:
        # { 'source_system': { 'source_code': 'unified_category' } }
        self.lookup = {}
        for source in self.mapping_df['source_system'].unique():
            source_subset = self.mapping_df[self.mapping_df['source_system'] == source]
            self.lookup[source] = source_subset.set_index('source_code')['unified_category'].to_dict()

    def map_legal_form(self, df: pd.DataFrame, source_system: str, code_col: str):
        """
        Translates source-specific codes into AICCON's unified categories.
        
        Args:
            df: The dataframe to transform.
            source_system: 'istat', 'eurostat', or 'runts'.
            code_col: The column name in df containing the raw code.
        """
        if source_system not in self.lookup:
            print(f"Warning: No mapping found for source system '{source_system}'")
            return df

        # Add the unified category column
        df['unified_category'] = df[code_col].map(self.lookup[source_system])
        
        # Add the English version as well (useful for international reporting)
        en_map = self.mapping_df[self.mapping_df['source_system'] == source_system].set_index('source_code')['unified_category_en'].to_dict()
        df['unified_category_en'] = df[code_col].map(en_map)
        
        # Fill unknowns to avoid empty slicers in PowerBI
        df['unified_category'] = df['unified_category'].fillna('Altro/Non classificato')
        
        return df

if __name__ == "__main__":
    # Quick test
    mapper = LegalFormMapper()
    test_data = pd.DataFrame({'raw_code': ['COOP_SOC_A', 'ODV', 'UNKNOWN']})
    result = mapper.map_legal_form(test_data, 'istat', 'raw_code')
    print(result)