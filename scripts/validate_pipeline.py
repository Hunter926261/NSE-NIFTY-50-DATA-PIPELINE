import pandas as pd
import sys
from pathlib import Path
from scripts.config import BASE_DIR
from scripts.logger import get_logger

logger = get_logger("pipeline_validation")

# CONFIGURATION 

MAX_ALLOWED_DATE_MISMATCH_PCT = 5.0     # Fail if mismatch > 5%
AUTO_ALIGN_IF_SAFE = True               # Auto-align only if within threshold

# FILE DETECTION

EQUITY_MASTER_DIR = BASE_DIR / "data" / "processed" / "equity" / "master"
INDEX_MASTER_DIR = BASE_DIR / "data" / "processed" / "index" / "master"

equity_files = sorted(EQUITY_MASTER_DIR.glob("nse_master_*.csv"))
index_files = sorted(INDEX_MASTER_DIR.glob("nifty50_index_master_*.csv"))

if not equity_files or not index_files:
    logger.error("Master files not found.")
    sys.exit(1)

equity_path = equity_files[-1]
index_path = index_files[-1]

logger.info(f"Using Equity Master: {equity_path.name}")
logger.info(f"Using Index Master: {index_path.name}")

# LOAD DATA (Optimized)

try:
    equity_df = pd.read_csv(equity_path, low_memory=False)
    index_df = pd.read_csv(index_path, low_memory=False)
except Exception as e:
    logger.error(f"Failed to load master files: {e}")
    sys.exit(1)

# Normalize date
equity_df['trade_date'] = pd.to_datetime(equity_df['trade_date']).dt.date
index_df['trade_date'] = pd.to_datetime(index_df['trade_date']).dt.date

# VALIDATION FUNCTIONS

def validate_equity(df):
    if df.duplicated(subset=["symbol", "trade_date"]).any():
        logger.error("Duplicate symbol-date rows found in equity master.")
        return False

    if df.isnull().values.any():
        logger.error("Missing values found in equity master.")
        return False

    logger.info("Equity validation passed.")
    return True


def validate_index(df):
    if df.duplicated(subset=["trade_date"]).any():
        logger.error("Duplicate trade_date rows found in index master.")
        return False

    if df.isnull().values.any():
        logger.error("Missing values found in index master.")
        return False

    logger.info("Index validation passed.")
    return True


def cross_validate(equity_df, index_df):
    equity_dates = pd.Index(equity_df['trade_date'].unique())
    index_dates = pd.Index(index_df['trade_date'].unique())

    common_dates = equity_dates.intersection(index_dates)
    missing_in_index = equity_dates.difference(index_dates)
    missing_in_equity = index_dates.difference(equity_dates)

    logger.info(f"Total Equity Dates: {len(equity_dates)}")
    logger.info(f"Total Index Dates: {len(index_dates)}")
    logger.info(f"Common Dates: {len(common_dates)}")

    if len(equity_dates) == 0:
        logger.error("Equity dataset contains no trading dates.")
        return False, equity_df, index_df

    impact_pct = (len(missing_in_index) / len(equity_dates)) * 100

    if len(missing_in_index) > 0:
        logger.warning(f"{len(missing_in_index)} equity dates missing in index dataset.")
        logger.warning(f"Date mismatch impact: {impact_pct:.2f}%")

    if len(missing_in_equity) > 0:
        logger.warning(f"{len(missing_in_equity)} index dates missing in equity dataset.")

    # Safety threshold check
    if impact_pct > MAX_ALLOWED_DATE_MISMATCH_PCT:
        logger.error(
            f"Date mismatch {impact_pct:.2f}% exceeds allowed threshold "
            f"({MAX_ALLOWED_DATE_MISMATCH_PCT}%). Possible incomplete dataset."
        )
        return False, equity_df, index_df

    # Auto align if safe
    if AUTO_ALIGN_IF_SAFE:
        equity_df = equity_df[equity_df['trade_date'].isin(common_dates)]
        index_df = index_df[index_df['trade_date'].isin(common_dates)]
        logger.info("Datasets aligned using common trading dates.")

    logger.info("Cross-dataset validation completed successfully.")
    return True, equity_df, index_df


# EXECUTION PIPELINE

if not validate_equity(equity_df):
    sys.exit(1)

if not validate_index(index_df):
    sys.exit(1)

status, equity_df, index_df = cross_validate(equity_df, index_df)

if not status:
    sys.exit(1)

logger.info("VALIDATION SUCCESS: Master datasets validated successfully.")
print("\nVALIDATION SUCCESS ✅\n")