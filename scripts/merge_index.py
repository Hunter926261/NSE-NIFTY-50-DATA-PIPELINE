import pandas as pd
from scripts.config import BASE_DIR
from scripts.logger import get_logger

logger = get_logger("index_merge")

INDEX_RAW_DIR = BASE_DIR / "data" / "raw" / "index"
INDEX_PROCESSED_DIR = BASE_DIR / "data" / "processed"
INDEX_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def extract_date_from_filename(filename):
    # ind_close_all_DDMMYYYY.csv
    date_str = filename[-12:-4]  # DDMMYYYY
    return pd.to_datetime(date_str, format="%d%m%Y")

def main():
    csv_files = list(INDEX_RAW_DIR.glob("ind_close_all_*.csv"))

    all_data = []

    for file in csv_files:
        try:
            df = pd.read_csv(file)

            # Filter NIFTY 50 row
            df_nifty = df[df["Index Name"] == "NIFTY Midcap 50"]

            if not df_nifty.empty:
                trade_date = extract_date_from_filename(file.name)
                df_nifty["trade_date"] = trade_date
                all_data.append(df_nifty)

            logger.info(f"Processed: {file.name}")

        except Exception as e:
            logger.error(f"Error processing {file.name} - {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        final_df.columns = [col.lower().strip() for col in final_df.columns]

        output_path = INDEX_PROCESSED_DIR / "nifty50_index_2015.csv"
        final_df.to_csv(output_path, index=False)

        logger.info("Index merge completed successfully.")
    else:
        logger.warning("No data merged.")

if __name__ == "__main__":
    main()
