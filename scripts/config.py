from pathlib import Path

# Base project directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Data directories
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
EXTRACTED_DIR = BASE_DIR / "data" / "extracted"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
METADATA_DIR = BASE_DIR / "data" / "metadata"

# Log directory
LOG_DIR = BASE_DIR / "logs"

# Create directories if not exist
for path in [RAW_DATA_DIR, EXTRACTED_DIR, PROCESSED_DIR, METADATA_DIR, LOG_DIR]:
    path.mkdir(parents=True, exist_ok=True)

# Date range (10 years)
START_DATE = "2015-01-01"
END_DATE   = "2025-12-31"
