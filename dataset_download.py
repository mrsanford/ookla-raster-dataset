# importing libraries
import boto3
import os
import logging
from botocore import UNSIGNED
from botocore.config import Config

# configuring the logger/error handling
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# establishing values and directories
BUCKET_NAME = 'ookla-open-data'
REGION_NAME = 'us-west-2'
LOCAL_DIRECTORY = '/Users/michellesanford/GitHub/geo-datasets/datasets/ookla_speedtest'
quarters = {1:'01', 2:'04', 3:'07', 4:'10'}
os.makedirs(LOCAL_DIRECTORY, exist_ok=True)

# creating the s3 client with anonymous token
def create_s3_client():
    return boto3.client('s3', region_name=REGION_NAME, config=Config(signature_version=UNSIGNED))

# creating the download file function and creating the loggers
def download_file(s3_client, s3_key, local_file_path):
    if os.path.exists(local_file_path):
        logger.info(f'File {local_file_path} already exists. Skipping download.')
        return
    logger.info(f'Downloading s3://{BUCKET_NAME}/{s3_key} to {local_file_path}')
    try:
        s3_client.download_file(BUCKET_NAME, s3_key, local_file_path)
        logger.info(f'Successfully downloaded {local_file_path}')
    except Exception as e:
        logger.error(f'Error downloading s3://{BUCKET_NAME}/{s3_key}: {e}')

# doing the actual donwloading; calling the S3 client, and putting the S3 filenames together
def download_files():
    s3_client = create_s3_client()
    formats = ['shapefiles', 'parquet']
    service_types = ['mobile', 'fixed']
    years = range(2019, 2020) # available years from 2019 - 2024
    for year in years:
        for quarter, month in quarters.items():
            for format_type in formats:
                for service_type in service_types:
                    filename = f'{year}-{month}-01_performance_{service_type}_tiles'
                    filename += '.zip' if format_type == 'shapefiles' else '.parquet'
                    s3_key = f'{format_type}/performance/type={service_type}/year={year}/quarter={quarter}/{filename}'
                    local_file_path = os.path.join(LOCAL_DIRECTORY, filename)
                    download_file(s3_client, s3_key, local_file_path)

# calling the function
if __name__ == "__main__":
    download_files()

# uv venv my_env