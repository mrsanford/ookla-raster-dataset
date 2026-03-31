import boto3
from pathlib import Path
from botocore import UNSIGNED
from botocore.client import BaseClient
from botocore.config import Config
from utils.helpers import GEOPARQUET_DIR, BUCKET_NAME, REGION_NAME, QUARTERS
from utils.loggers import setup_custom_logger

logger = setup_custom_logger("DOWNLOAD")


# Creating the s3 client with anonymous token
def create_s3_client() -> BaseClient:
    return boto3.client(
        "s3", region_name=REGION_NAME, config=Config(signature_version=UNSIGNED)
    )


# Creating the download file function and creating the loggers
def prepare_download(s3_client: BaseClient, s3_key: str) -> None:
    """
    Checks directory, existence check, and S3 download
    """
    # Checking base_dir Path object
    base_dir = Path(GEOPARQUET_DIR)
    base_dir.mkdir(parents=True, exist_ok=True)
    # Extracting filename and defining local path
    filename = Path(s3_key).name
    local_file_path = base_dir / filename
    if local_file_path.exists():
        logger.info(f"File {local_file_path} already exists. Skipping download.")
        return
    # Performs Download
    logger.info(f"Downloading s3://{BUCKET_NAME}/{s3_key} to {local_file_path}")
    try:
        s3_client.download_file(BUCKET_NAME, s3_key, str(local_file_path))
        logger.info(f"Successfully downloaded {local_file_path}")
    except Exception as e:
        logger.error(f"Error downloading s3://{BUCKET_NAME}/{s3_key}: {e}")


def generate_s3_key(year, month, quarter, service, fmt) -> str:
    """Helper to construct the S3 key string."""
    ext = ".zip" if fmt == "shapefiles" else ".parquet"
    filename = f"{year}-{month}-01_performance_{service}_tiles{ext}"
    return f"{fmt}/performance/type={service}/year={year}/quarter={quarter}/{filename}"


def download_files() -> None:
    """
    Downloads the performance data files from the Ookla S3 bucket to a local directory
    Note: there is a similarity in the naming convention download_file and download_files
    I can't do anything to change it becuase that's just what the botocore requires.
    Be aware of the differences
    """
    s3_client = create_s3_client()
    formats = ["shapefiles", "parquet"]
    service_types = ["mobile", "fixed"]
    years = range(2019, 2026)
    for year in years:
        for quarter, month in QUARTERS.items():
            for format_type in formats:
                for service_type in service_types:
                    s3_key = generate_s3_key(
                        year, month, quarter, service_type, format_type
                    )
                    prepare_download(s3_client, s3_key)
                    # filename = f"{year}-{month}-01_performance_{service_type}_tiles"
                    # filename += ".zip" if format_type == "shapefiles" else ".parquet"
                    # s3_key = f"{format_type}/performance/type={service_type}/year={year}/quarter={quarter}/{filename}"
                    # prepare_download(s3_client, s3_key)
