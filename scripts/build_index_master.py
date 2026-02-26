import pandas as pd
from pathlib import Path
from scripts.config import BASE_DIR
from scripts.logger import get_logger
import re

logger = get_logger("index_master")

# 📂 Equity data is seperated yearly & master section
INDEX_YEARLY_DIR = BASE_DIR / "data" / "processed" / "index" / "yearly"
INDEX_MASTER_DIR = BASE_DIR / "data" / "processed" / "index" / "master"

# Ensure master directory exists
INDEX_MASTER_DIR.mkdir(parents=True, exist_ok=True)


def extract_year(filename: str):
    """
    Extract year from filename like:
    nifty50_index_2015.csv
    """
    match = re.search(r"\d{4}", filename)
    if match:
        return int(match.group())
    return None


def main():

    # 🔄 Read from yearly folder
    yearly_files = sorted(
        INDEX_YEARLY_DIR.glob("nifty50_index_*.csv")
    )

    if not yearly_files:
        logger.warning("No index yearly files found.")
        return

    all_data = []
    years = []

    for file in yearly_files:
        try:
            df = pd.read_csv(file)
            all_data.append(df)
            logger.info(f"Loaded: {file.name}")

            year = extract_year(file.name)
            if year:
                years.append(year)

        except Exception as e:
            logger.error(f"Error loading {file.name} - {e}")

    if not all_data:
        logger.warning("No data found for index master build.")
        return

    if not years:
        logger.error("Could not detect year range from filenames.")
        return

    # 🔗 Merge all data
    master_df = pd.concat(all_data, ignore_index=True)

    # 🧹 Remove duplicates (safe practice)
    master_df.drop_duplicates(inplace=True)

    # 📅 Detect year range
    start_year = min(years)
    end_year = max(years)

    # 📝 Dynamic master filename
    master_filename = f"nifty50_index_master_{start_year}_{end_year}.csv"
    master_path = INDEX_MASTER_DIR / master_filename

    # 💾 Save master file
    master_df.to_csv(master_path, index=False)

    logger.info("Index master dataset created successfully.")
    logger.info(f"Saved to: {master_path}")


if __name__ == "__main__":
    main()