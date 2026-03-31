import logging
import sys
from pathlib import Path


def setup_custom_logger(name: str, log_level: int = logging.INFO):
    """
    Standardized logger for pipeline, returns INFO level to logs
    """
    logging.getLogger("botocore").setLevel(
        logging.WARNING
    )  # Suppress botocore checksum spam
    logging.getLogger("s3transfer").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Log Directory
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{name}.log"
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        # Saving Logs to (./logs/{log_name})
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger
