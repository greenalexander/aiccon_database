import duckdb
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv

# Why these specific tests?
# Referential Integrity: If you have a row for a NUTS code like IT999 that isn't in your mapping, it will show up as (Blank) in PowerBI. This test catches that early.
# The "5% Margin" Test: Official statistics (especially from ISTAT) often have small discrepancies between the sum of provinces and the regional total due to "undistributed" data or rounding. This test alerts you only if the gap is massive (over 5%).
# Freshness: It’s easy to run a pipeline and realize later that the API didn't have the new year yet. This tells you immediately if 2023 or 2024 data is missing.

class DatabaseTester:
    def __init__(self):
        load_dotenv()
        with open("settings.yaml", "r") as f:
            self.config = yaml.safe_load(f)
            
        self.root_path = Path(os.getenv("SHAREPOINT_ROOT", "."))
        self.db_path = self.root_path / self.config['sharepoint']['database_dir'] / self.config['sharepoint']['database_filename']
        self.con = duckdb.connect(str(self.db_path))

    def run_test(self, name, sql, expected_condition="zero"):
        """Helper to run a SQL query and check if the result is as expected."""
        result = self.con.execute(sql).df()
        
        if expected_condition == "zero":
            passed = len(result) == 0
        else:
            passed = not result.empty

        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {name}")
        
        if not passed:
            print(f"   Details: {result.to_dict(orient='records')[:3]}...") # Show first 3 errors
        return passed

    def perform_checks(self):
        print(f"\n--- Running Integrity Checks on {self.db_path.name} ---")
        
        # TEST 1: Orphaned NUTS Codes
        # Checks if there are any codes in the fact table that DON'T exist in our dim_geography
        self.run_test(
            "Referential Integrity (Geography)",
            """
            SELECT DISTINCT nuts_code 
            FROM fact_social_economy 
            WHERE nuts_code NOT IN (SELECT nuts_code FROM dim_geography)
            """
        )

        # TEST 2: Logical Consistency (Sub-total vs Total)
        # Checks if sum of NUTS3 (Provinces) is greater than NUTS2 (Region) 
        # (A common error if data is double-counted)
        self.run_test(
            "Logical Consistency (Regional Totals)",
            """
            WITH prov_sum AS (
                SELECT nuts_code[:4] as reg_prefix, time, SUM(value) as val 
                FROM fact_social_economy WHERE nuts_level = 3 GROUP BY 1, 2
            ),
            reg_val AS (
                SELECT nuts_code, time, value 
                FROM fact_social_economy WHERE nuts_level = 2
            )
            SELECT p.reg_prefix, p.val as prov_total, r.value as reg_total
            FROM prov_sum p 
            JOIN reg_val r ON p.reg_prefix = r.nuts_code AND p.time = r.time
            WHERE p.val > (r.value * 1.05) -- Allow 5% margin for rounding/errors
            """
        )

        # TEST 3: Null Values
        # Crucial for PowerBI: ensures no blank slices in your charts
        self.run_test(
            "Data Quality (Null Values)",
            "SELECT * FROM fact_social_economy WHERE value IS NULL OR unified_category IS NULL"
        )

        # TEST 4: Recent Data Presence
        # Ensures the pipeline actually fetched new data (e.g., check for current or last year)
        import datetime
        last_year = str(datetime.datetime.now().year - 1)
        self.run_test(
            f"Freshness (Contains {last_year} data)",
            f"SELECT * FROM fact_social_economy WHERE time = '{last_year}'",
            expected_condition="not_empty"
        )

if __name__ == "__main__":
    tester = DatabaseTester()
    tester.perform_checks()