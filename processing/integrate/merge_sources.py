import pandas as pd

class DataIntegrator:
    def __init__(self):
        pass

    def combine_social_economy(self, istat_df, eurostat_df):
        """
        Combines ISTAT and Eurostat dataframes. 
        Ensures columns are aligned (Source, Year, NUTS_Code, Category, Value).
        """
        # 1. Standardise Columns for ISTAT
        istat_clean = istat_df[[
            'time', 'nuts_code', 'nuts_level', 'unified_category', 'value'
        ]].copy()
        istat_clean['source_priority'] = 1
        istat_clean['source_name'] = 'ISTAT'

        # 2. Standardise Columns for Eurostat
        eurostat_clean = eurostat_df[[
            'time', 'nuts_code', 'nuts_level', 'unified_category', 'value'
        ]].copy()
        eurostat_clean['source_priority'] = 2
        eurostat_clean['source_name'] = 'Eurostat'

        # 3. Concatenate
        combined = pd.concat([istat_clean, eurostat_clean], ignore_index=True)
        
        # 4. Handle Overlaps (De-duplication)
        # If we have both ISTAT and Eurostat for the same Year/NUTS/Category,
        # we sort by priority and keep the first (Priority 1).
        combined = combined.sort_values(by=['time', 'nuts_code', 'unified_category', 'source_priority'])
        combined = combined.drop_duplicates(subset=['time', 'nuts_code', 'unified_category'], keep='first')

        return combined