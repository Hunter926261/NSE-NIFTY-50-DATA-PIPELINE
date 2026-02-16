import sys
import pandas as pd
from pathlib import Path
from scripts.config import EXTRACTED_DIR, PROCESSED_DIR
from scripts.logger import get_logger

logger = get_logger("merge")

# -------------------------------
# Accept YEAR as command argument
# -------------------------------
if len(sys.argv) != 2:
    print("Usage: python -m scripts.merge_bhavcopy YEAR")
    sys.exit(1)

target_year = sys.argv[1]


def extract_date_from_filename(filename):
    """
    Example filename:
    cm01JAN2015bhav.csv
    Extracts 01JAN2015
    """
    date_str = filename[2:11]
    return pd.to_datetime(date_str, format="%d%b%Y")


def main():

    # ---------------------------------
    # Filter files only for given year
    # ---------------------------------
    csv_files = list(EXTRACTED_DIR.glob(f"cm*{target_year}bhav.csv"))

    if not csv_files:
        logger.warning(f"No files found for year {target_year}")
        return

    all_data = []

    for file in csv_files:
        try:
            df = pd.read_csv(file)

            # Filter only Equity series
            if "SERIES" in df.columns:
                df = df[df["SERIES"] == "EQ"]

            # Add trade date
            trade_date = extract_date_from_filename(file.name)
            df["TRADE_DATE"] = trade_date

            all_data.append(df)

            logger.info(f"Processed: {file.name}")

        except Exception as e:
            logger.error(f"Error processing {file.name} - {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        # -------------------------------
        # Standardize column names
        # -------------------------------
        final_df.columns = [col.lower().strip() for col in final_df.columns]

        # -------------------------------
        # Drop unwanted columns
        # -------------------------------
        cols_to_drop = ["unnamed: 13"]
        final_df = final_df.drop(
            columns=[col for col in cols_to_drop if col in final_df.columns]
        )

        # -------------------------------
        # Convert numeric columns
        # -------------------------------
        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "last",
            "prevclose",
            "tottrdqty",
            "tottrdval",
            "totaltrades",
        ]

        for col in numeric_cols:
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors="coerce")

        # -------------------------------
        # Create equity folder if needed
        # -------------------------------
        equity_dir = PROCESSED_DIR / "equity"
        equity_dir.mkdir(parents=True, exist_ok=True)

        # -------------------------------
        # Save output
        # -------------------------------
        output_path = equity_dir / f"nse_{target_year}.csv"
        final_df.to_csv(output_path, index=False)

        logger.info(f"Year {target_year} merge completed successfully.")
        logger.info(f"Saved to: {output_path}")

    else:
        logger.warning("No data to merge.")


if __name__ == "__main__":
    main()
