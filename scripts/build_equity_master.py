import pandas as pd
from pathlib import Path
from scripts.config import BASE_DIR
from scripts.logger import get_logger
import re

logger = get_logger("equity_master")

# 📂 Equity data is seperated yearly & master section
EQUITY_YEARLY_DIR = BASE_DIR / "data" / "processed" / "equity" / "yearly"
EQUITY_MASTER_DIR = BASE_DIR / "data" / "processed" / "equity" / "master"

# Ensure master directory exists
EQUITY_MASTER_DIR.mkdir(parents=True, exist_ok=True)


def extract_year(filename: str):
    """
    Extract year from filename like:
    nse_2015.csv
    nse_2016.csv
    """
    match = re.search(r"\d{4}", filename)
    if match:
        return int(match.group())
    return None


def main():
    # 🔄 Read from yearly folder
    yearly_files = sorted(EQUITY_YEARLY_DIR.glob("nse_*.csv"))

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
        logger.warning("No data found for master build.")
        return

    if not years:
        logger.error("Could not detect year range from filenames.")
        return

    # 🔗 Merge all yearly data
    master_df = pd.concat(all_data, ignore_index=True)

    # 📅 Detect year range
    start_year = min(years)
    end_year = max(years)

    # 📝 Dynamic master filename
    master_filename = f"nse_master_{start_year}_{end_year}.csv"
    master_path = EQUITY_MASTER_DIR / master_filename

    # 💾 Save master file in master folder
    master_df.to_csv(master_path, index=False)

    logger.info(f"Master equity dataset created successfully: {master_filename}")


if __name__ == "__main__":
    main()