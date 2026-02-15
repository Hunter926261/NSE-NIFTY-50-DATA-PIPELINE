import zipfile
from scripts.config import RAW_DATA_DIR, EXTRACTED_DIR
from scripts.logger import get_logger

logger = get_logger("extract")

def extract_zip(zip_path, extract_to):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        logger.info(f"Extracted: {zip_path.name}")
    except zipfile.BadZipFile:
        logger.error(f"Corrupt ZIP file: {zip_path.name}")
    except Exception as e:
        logger.error(f"Error extracting {zip_path.name} - {e}")

def main():
    zip_files = list(RAW_DATA_DIR.glob("*.zip"))

    for zip_file in zip_files:
        # Extract only if corresponding CSV not already extracted
        expected_csv = zip_file.stem  # removes .zip

        extracted_csv_path = EXTRACTED_DIR / expected_csv

        if not extracted_csv_path.exists():
            extract_zip(zip_file, EXTRACTED_DIR)

    logger.info("Extraction Completed.")


if __name__ == "__main__":
    main()
