import duckdb
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv

def build_database():
    load_dotenv("config/.env")
    with open("config/settings.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    root_path = Path(os.getenv("SHAREPOINT_ROOT", "."))
    db_path = root_path / config['sharepoint']['database_dir'] / config['sharepoint']['database_filename']
    proc_dir = root_path / config['sharepoint']['processed_dir']
    
    # Ensure database directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to DuckDB (creates the file if it doesn't exist)
    con = duckdb.connect(str(db_path))
    
    print(f"Building database at: {db_path}")
    
    # 1. Create Dimensions (Step 12)
    with open("database/schema/dimensions.sql", "r") as f:
        con.execute(f.read())
    
    # 2. Create Fact Tables (Step 13)
    with open("database/schema/fact_tables.sql", "r") as f:
        con.execute(f.read())
        
    # 3. Load the processed Parquet data into the Fact Table
    fact_file = proc_dir / "fact_social_economy.parquet"
    if fact_file.exists():
        con.execute(f"INSERT INTO fact_social_economy SELECT * FROM read_parquet('{fact_file}')")
        print("Successfully loaded social economy facts.")
    else:
        print("Warning: No processed parquet file found to load.")
        
    # 4. Create Views (Step 14)
    with open("database/schema/views.sql", "r") as f:
        con.execute(f.read())
        
    con.close()
    print("Database build complete.")

if __name__ == "__main__":
    build_database()