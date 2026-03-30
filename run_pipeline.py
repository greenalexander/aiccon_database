# run_pipeline.py
import subprocess
import sys

def run_step(name, path):
    print(f"\n>>> STARTING STEP: {name}")
    result = subprocess.run([sys.executable, path])
    if result.returncode != 0:
        print(f"!!! ERROR in {name}. Stopping pipeline.")
        sys.exit(1)

if __name__ == "__main__":
    print("AICCON DATA ENGINE - FULL REFRESH")
    
    # Phase 1: Ingest
    run_step("Eurostat Fetch", "ingestion/api_sources/social_economy/eurostat.py")
    run_step("ISTAT Fetch", "ingestion/api_sources/social_economy/istat.py")
    
    # Phase 2: Process
    run_step("Processing Pipeline", "processing/pipeline.py")
    
    # Phase 3: Database
    run_step("Database Build", "database/build_db.py")
    run_step("Integrity Tests", "database/tests/integrity_checks.py")
    
    print("\n✅ SUCCESS: All data is updated and verified.")