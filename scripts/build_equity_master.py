import pandas as pd
from pathlib import Path
from scripts.config import BASE_DIR
from scripts.logger import get_logger

logger = get_logger("equity_master")

EQUITY_DIR = BASE_DIR / "data" / "processed" / "equity"
MASTER_PATH = EQUITY_DIR / "nse_master_2015_2016.csv"

def main():
    yearly_files = sorted(EQUITY_DIR.glob("nse_*.csv"))

    all_data = []

    for file in yearly_files:
        try:
            df = pd.read_csv(file)
            all_data.append(df)
            logger.info(f"Loaded: {file.name}")
        except Exception as e:
            logger.error(f"Error loading {file.name} - {e}")

    if all_data:
        master_df = pd.concat(all_data, ignore_index=True)
        master_df.to_csv(MASTER_PATH, index=False)
        logger.info("Master equity dataset created successfully.")
    else:
        logger.warning("No data found for master build.")

if __name__ == "__main__":
    main()
