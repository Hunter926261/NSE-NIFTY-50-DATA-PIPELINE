import pandas as pd
from pathlib import Path
from scripts.config import EXTRACTED_DIR, PROCESSED_DIR
from scripts.logger import get_logger

logger = get_logger("merge")

def extract_date_from_filename(filename):
    # Example: cm01JAN2015bhav.csv
    date_str = filename[2:11]  # 01JAN2015
    return pd.to_datetime(date_str, format="%d%b%Y")

def main():
    csv_files = list(EXTRACTED_DIR.glob("cm*bhav.csv"))

    all_data = []

    for file in csv_files:
        try:
            df = pd.read_csv(file)

            # Filter only Equity series
            df = df[df["SERIES"] == "EQ"]

            # Add date column
            trade_date = extract_date_from_filename(file.name)
            df["TRADE_DATE"] = trade_date

            all_data.append(df)

            logger.info(f"Processed: {file.name}")

        except Exception as e:
            logger.error(f"Error processing {file.name} - {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        # Standardize column names
        final_df.columns = [col.lower().strip() for col in final_df.columns]

        # Drop unwanted columns
        cols_to_drop = ["unnamed: 13"]
        final_df = final_df.drop(columns=[col for col in cols_to_drop if col in final_df.columns])

        # Ensure numeric columns are correct
        numeric_cols = ["open", "high", "low", "close", "last", "prevclose", "tottrdqty", "tottrdval", "totaltrades"]

        for col in numeric_cols:
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors="coerce")


        output_path = PROCESSED_DIR / "nse_2015_full.csv"
        final_df.to_csv(output_path, index=False)

        logger.info("Year merge completed successfully.")
    else:
        logger.warning("No data to merge.")

if __name__ == "__main__":
    main()
