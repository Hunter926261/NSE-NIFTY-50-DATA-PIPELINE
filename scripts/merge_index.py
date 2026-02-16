import sys
import pandas as pd
from pathlib import Path
from scripts.config import BASE_DIR
from scripts.logger import get_logger

logger = get_logger("index_merge")

# -------------------------------
# Validate Command Line Argument
# -------------------------------
if len(sys.argv) != 2:
    print("Usage: python -m scripts.merge_index YEAR")
    sys.exit(1)

target_year = sys.argv[1]

# -------------------------------
# Paths
# -------------------------------
INDEX_RAW_DIR = BASE_DIR / "data" / "raw" / "index"

INDEX_PROCESSED_DIR = BASE_DIR / "data" / "processed" / "index"
INDEX_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Extract date from filename
# Example: ind_close_all_DDMMYYYY.csv
# -------------------------------
def extract_date_from_filename(filename):
    date_str = filename[-12:-4]  # DDMMYYYY
    return pd.to_datetime(date_str, format="%d%m%Y")


# -------------------------------
# Main Logic
# -------------------------------
def main():

    # Filter files for target year
    csv_files = list(
        INDEX_RAW_DIR.glob(f"ind_close_all_*{target_year}.csv")
    )

    if not csv_files:
        logger.warning(f"No files found for year {target_year}")
        return

    all_data = []

    for file in csv_files:
        try:
            df = pd.read_csv(file)

            # -------------------------------
            # Clean columns
            # -------------------------------
            df.columns = df.columns.str.strip()

            if "Index Name" not in df.columns:
                logger.warning(f"'Index Name' column missing in {file.name}")
                continue

            # -------------------------------
            # Normalize Index Name column
            # -------------------------------
            df["Index Name"] = (
                df["Index Name"]
                .astype(str)
                .str.strip()
                .str.upper()
            )

            # -------------------------------
            # Filter NIFTY 50 & MIDCAP 50
            # -------------------------------
            df_nifty = df[
                df["Index Name"].isin(
                    ["NIFTY 50"]
                )
            ]

            if not df_nifty.empty:
                trade_date = extract_date_from_filename(file.name)

                df_nifty = df_nifty.copy()
                df_nifty["trade_date"] = trade_date

                all_data.append(df_nifty)

            logger.info(f"Processed: {file.name}")

        except Exception as e:
            logger.error(f"Error processing {file.name} - {e}")

    # -------------------------------
    # Save Final Output
    # -------------------------------
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        # Clean column names for output
        final_df.columns = [
            col.lower().strip().replace(" ", "_")
            for col in final_df.columns
        ]

        output_path = (
            INDEX_PROCESSED_DIR /
            f"nifty50_index_{target_year}.csv"
        )

        final_df.to_csv(output_path, index=False)

        logger.info(
            f"Index merge completed successfully for {target_year}"
        )
        logger.info(f"Saved to: {output_path}")

    else:
        logger.warning(f"No data merged for {target_year}")


# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    main()