import requests
from datetime import datetime, timedelta
import time
import sys
from scripts.config import RAW_DATA_DIR
from scripts.logger import get_logger

logger = get_logger("download")

BASE_HOME = "https://www.nseindia.com"

OLD_BASE = "https://archives.nseindia.com/content/historical/EQUITIES"
NEW_BASE = "https://archives.nseindia.com/content/cm"

# -----------------------------
# CLI arguments
# -----------------------------
if len(sys.argv) != 3:
    print("Usage: python -m scripts.download_bhavcopy START_YEAR END_YEAR")
    sys.exit(1)

start_year = int(sys.argv[1])
end_year = int(sys.argv[2])

START_DATE = f"{start_year}-01-01"
END_DATE   = f"{end_year}-12-31"


# -----------------------------
# URL Builder
# -----------------------------
def build_old_url(date):
    year = date.strftime("%Y")
    month = date.strftime("%b").upper()
    day = date.strftime("%d")
    file_name = f"cm{day}{month}{year}bhav.csv.zip"
    url = f"{OLD_BASE}/{year}/{month}/{file_name}"
    return url, file_name


def build_new_url(date):
    date_str = date.strftime("%Y%m%d")
    file_name = f"BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
    url = f"{NEW_BASE}/{file_name}"
    return url, file_name


# -----------------------------
# Main
# -----------------------------
def main():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end   = datetime.strptime(END_DATE, "%Y-%m-%d")

    current = start

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": BASE_HOME,
        "Accept-Language": "en-US,en;q=0.9"
    })

    session.get(BASE_HOME)  # Initialize cookies

    while current <= end:

        if current.year < 2024:
            url, file_name = build_old_url(current)
        else:
            url, file_name = build_new_url(current)

        save_path = RAW_DATA_DIR / file_name

        try:
            if not save_path.exists():
                response = session.get(url)

                if response.status_code == 200:
                    with open(save_path, "wb") as f:
                        f.write(response.content)
                    logger.info(f"Downloaded: {file_name}")
                else:
                    logger.warning(f"Skipped/Blocked: {file_name}")

            current += timedelta(days=1)
            time.sleep(1.5)

        except Exception as e:
            logger.error(f"Error: {e}")
            current += timedelta(days=1)

    logger.info("Download Completed.")


if __name__ == "__main__":
    main()
