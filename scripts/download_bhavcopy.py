import requests
from datetime import datetime, timedelta
import time
from scripts.config import RAW_DATA_DIR
from scripts.logger import get_logger

logger = get_logger("download")

BASE_ARCHIVE = "https://archives.nseindia.com/content/historical/EQUITIES"
BASE_HOME = "https://www.nseindia.com"

START_DATE = "2015-01-01"
END_DATE   = "2015-12-31"


def build_url(date):
    year = date.strftime("%Y")
    month = date.strftime("%b").upper()
    day = date.strftime("%d")
    file_name = f"cm{day}{month}{year}bhav.csv.zip"
    url = f"{BASE_ARCHIVE}/{year}/{month}/{file_name}"
    return url, file_name


def main():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end   = datetime.strptime(END_DATE, "%Y-%m-%d")

    current = start

    session = requests.Session()

    # Step 1: Hit homepage to get cookies
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": BASE_HOME,
        "Connection": "keep-alive"
    })

    session.get(BASE_HOME)  # Initialize cookies

    while current <= end:
        url, file_name = build_url(current)
        save_path = RAW_DATA_DIR / file_name

        try:
            if not save_path.exists():
                response = session.get(url)

                if response.status_code == 200:
                    with open(save_path, "wb") as f:
                        f.write(response.content)
                    logger.info(f"Downloaded: {file_name}")
                else:
                    logger.warning(f"Blocked/No Data: {url}")

            current += timedelta(days=1)
            time.sleep(1.5)  # Slightly longer delay for safety

        except Exception as e:
            logger.error(f"Error: {e}")
            current += timedelta(days=1)

    logger.info("Download Completed.")


if __name__ == "__main__":
    main()
