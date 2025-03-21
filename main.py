from src.helpers import (
    GEOPARQUET_DIR,
    TEST_PARQUET_FILE,
    BAND_COLUMN_NAME,
    RASTER_OUTPUT_DIR,
)

# from src.dataset_download import download_files
from src.convert_populate import read_parquet, populate_array
from src.make_raster import make_raster_profile, write_raster
import logging
import sys
import os

# Logging setup
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def main():
    try:
        # download_files() <- This works but not trying to continue letting it run

        parquet_data_path = os.path.join(GEOPARQUET_DIR, TEST_PARQUET_FILE)
        output_raster_path = os.path.join(RASTER_OUTPUT_DIR, "total_band_raster_10.tif")

        logger.info(f"Reading parquet file: {parquet_data_path}")
        gdf = read_parquet(parquet_data_path)
        if gdf is None or gdf.empty:
            logger.error("GeoDataFrame is empty or could not be loaded.")
            return
        logger.info("GeoDataFrame successfully loaded.")

        array_data = populate_array(gdf, BAND_COLUMN_NAME)

        logger.info(f"Array populated with shape: {array_data.shape}")

        profile = make_raster_profile()
        expected_shape = (profile["count"], profile["height"], profile["width"])
        if array_data.shape != expected_shape:
            logger.warning(
                f"Array shape {array_data.shape} does not match expected profile shape {expected_shape}."
            )

        logger.info(f"Writing raster to: {output_raster_path}")
        write_raster(array_data, profile, str(output_raster_path))

        logger.info("Raster creation process completed successfully.")

    except Exception as e:
        logger.exception(f"Unhandled error in main process: {e}")


if __name__ == "__main__":
    main()
