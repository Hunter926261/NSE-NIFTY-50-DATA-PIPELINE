import requests
from datetime import datetime, timedelta
import time
from pathlib import Path
from scripts.config import BASE_DIR
from scripts.logger import get_logger

logger = get_logger("index_download")

BASE_ARCHIVE = "https://archives.nseindia.com/content/indices"
BASE_HOME = "https://www.nseindia.com"

INDEX_RAW_DIR = BASE_DIR / "data" / "raw" / "index"
INDEX_RAW_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = "2015-01-01"
END_DATE   = "2015-12-31"  # test 1 year first


def build_url(date):
    date_str = date.strftime("%d%m%Y")
    file_name = f"ind_close_all_{date_str}.csv"
    url = f"{BASE_ARCHIVE}/{file_name}"
    return url, file_name


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
        url, file_name = build_url(current)
        save_path = INDEX_RAW_DIR / file_name

        try:
            if not save_path.exists():
                response = session.get(url)

                if response.status_code == 200:
                    with open(save_path, "wb") as f:
                        f.write(response.content)
                    logger.info(f"Downloaded: {file_name}")
                else:
                    logger.warning(f"Skipped: {file_name}")

            current += timedelta(days=1)
            time.sleep(1.5)

        except Exception as e:
            logger.error(f"Error: {e}")
            current += timedelta(days=1)

    logger.info("Index download completed.")


if __name__ == "__main__":
    main()
