import pandas as pd
from pathlib import Path
from scripts.config import BASE_DIR
from scripts.logger import get_logger

logger = get_logger("index_master")

# -------------------------------
# Paths
# -------------------------------
INDEX_DIR = BASE_DIR / "data" / "processed" / "index"

MASTER_PATH = INDEX_DIR / "nifty50_index_master_2015_2016.csv"


# -------------------------------
# Main Logic
# -------------------------------
def main():

    # Get all yearly index files
    yearly_files = sorted(
        INDEX_DIR.glob("nifty50_index_*.csv")
    )

    if not yearly_files:
        logger.warning("No index yearly files found.")
        return

    all_data = []

    for file in yearly_files:
        try:
            df = pd.read_csv(file)
            all_data.append(df)
            logger.info(f"Loaded: {file.name}")

        except Exception as e:
            logger.error(f"Error loading {file.name} - {e}")

    # -------------------------------
    # Build Master Dataset
    # -------------------------------
    if all_data:
        master_df = pd.concat(all_data, ignore_index=True)

        # Optional: Remove duplicate rows (safe practice)
        master_df.drop_duplicates(inplace=True)

        master_df.to_csv(MASTER_PATH, index=False)

        logger.info("Index master dataset created successfully.")
        logger.info(f"Saved to: {MASTER_PATH}")

    else:
        logger.warning("No data found for index master build.")


# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    main()
