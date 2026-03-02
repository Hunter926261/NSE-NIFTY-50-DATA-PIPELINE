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


# -----------------------------------------
# Extract trade date from filename (OLD + NEW)
# -----------------------------------------
def extract_date_from_filename(filename):

    # OLD FORMAT
    if filename.startswith("cm"):
        date_str = filename[2:11]  # 01JAN2015
        return pd.to_datetime(date_str, format="%d%b%Y")

    # NEW FORMAT (UDiFF)
    elif filename.startswith("BhavCopy"):
        parts = filename.split("_")
        date_str = parts[6]  # YYYYMMDD
        return pd.to_datetime(date_str, format="%Y%m%d")

    else:
        raise ValueError(f"Unknown filename format: {filename}")


# -----------------------------------------
# Normalize OLD format
# -----------------------------------------
def normalize_old_format(df):

    df.columns = df.columns.str.lower().str.strip()

    df = df[df["series"] == "EQ"]

    df = df.rename(columns={
        "symbol": "symbol",
        "series": "series",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "last": "last",
        "prevclose": "prevclose",
        "tottrdqty": "tottrdqty",
        "tottrdval": "tottrdval",
        "totaltrades": "totaltrades",
        "isin": "isin",
        "timestamp": "timestamp"
    })

    return df


# -----------------------------------------
# Normalize NEW UDiFF format
# -----------------------------------------
def normalize_udiff_format(df):

    df.columns = df.columns.str.lower().str.strip()

    # Filter only Cash Market (CM) + Equity
    df = df[df["sgmt"] == "CM"]
    df = df[df["sctysrs"] == "EQ"]

    df = df.rename(columns={
        "tckrsymb": "symbol",
        "sctysrs": "series",
        "opnpric": "open",
        "hghpric": "high",
        "lwpric": "low",
        "clspric": "close",
        "lastpric": "last",
        "prvsclsgpric": "prevclose",
        "ttltradgvol": "tottrdqty",
        "ttltrfval": "tottrdval",
        "ttlnboftxsexctd": "totaltrades",
        "isin": "isin",
        "traddt": "timestamp"
    })

    return df


def main():

    csv_files = list(EXTRACTED_DIR.glob("*.csv"))

    if not csv_files:
        logger.warning("No CSV files found.")
        return

    all_data = []

    for file in csv_files:
        try:
            trade_date = extract_date_from_filename(file.name)

            if str(trade_date.year) != target_year:
                continue

            df = pd.read_csv(file)

            df.columns = df.columns.str.lower().str.strip()

            # Detect format automatically
            if "symbol" in df.columns and "series" in df.columns:
                df = normalize_old_format(df)

            elif "tckrsymb" in df.columns:
                df = normalize_udiff_format(df)

            else:
                logger.warning(f"Unknown structure in {file.name}")
                continue

            # Add trade_date column
            df["trade_date"] = trade_date

            # Keep only standardized columns
            required_columns = [
                "symbol",
                "series",
                "open",
                "high",
                "low",
                "close",
                "last",
                "prevclose",
                "tottrdqty",
                "tottrdval",
                "totaltrades",
                "isin",
                "timestamp",
                "trade_date",
            ]

            df = df[required_columns]

            all_data.append(df)

            logger.info(f"Processed: {file.name}")

        except Exception as e:
            logger.error(f"Error processing {file.name} - {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        equity_dir = PROCESSED_DIR / "equity" / "yearly"
        equity_dir.mkdir(parents=True, exist_ok=True)

        output_path = equity_dir / f"nse_{target_year}.csv"
        final_df.to_csv(output_path, index=False)

        logger.info(f"Year {target_year} merge completed successfully.")
        logger.info(f"Saved to: {output_path}")

    else:
        logger.warning(f"No data found for year {target_year}.")


if __name__ == "__main__":
    main()