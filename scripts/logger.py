import logging
from scripts.config import LOG_DIR

log_file = LOG_DIR / "pipeline.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_logger(name):
    return logging.getLogger(name)
