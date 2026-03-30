import pandas as pd
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv
import logging
import datetime

# Import the mappers we created in previous steps
from processing.harmonise.nuts_mapper import NutsMapper
from processing.harmonise.legal_form_normaliser import LegalFormMapper
from processing.integrate.merge_sources import DataIntegrator

class SocialEconomyPipeline:
    def __init__(self):
        load_dotenv()
        with open("settings.yaml", "r") as f:
            self.config = yaml.safe_load(f)
            
        self.root_path = Path(os.getenv("SHAREPOINT_ROOT", "."))
        
        # LOGGING SETUP
        log_path = self.root_path / self.config['sharepoint']['log_filename']
        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a' # Append to the log file
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("--- Pipeline Session Started ---")

    def process_istat(self):
        processed_dfs = []
        istat_files = list(self.raw_dir.glob("istat_*.parquet"))
        
        if not istat_files:
            self.logger.warning("No ISTAT raw files found in directory. Skipping ISTAT processing.")
            return pd.DataFrame()

        for file in istat_files:
            try:
                self.logger.info(f"Processing ISTAT file: {file.name}")
                df = pd.read_parquet(file)
                
                # Check for empty dataframes early
                if df.empty:
                    self.logger.warning(f"File {file.name} is empty. Skipping.")
                    continue

                # ... (Mapping logic from before) ...
                
                self.logger.info(f"Successfully mapped {len(df)} rows from {file.name}")
                processed_dfs.append(df)

            except Exception as e:
                # This catches the "Where" and "Why"
                self.logger.error(f"Failed to process {file.name}. Error: {str(e)}")
                # We continue the loop so other files can still be processed
                continue
                
        return pd.concat(processed_dfs, ignore_index=True) if processed_dfs else pd.DataFrame()

    def run(self):
        try:
            self.logger.info("Starting Social Economy Processing Pipeline...")
            # ... (the rest of your run logic)
            self.logger.info("Pipeline run finished successfully.")
        except Exception as e:
            self.logger.critical(f"PIPELINE CRASHED: {str(e)}")
            raise # Still raise it so you see the red text in the terminal