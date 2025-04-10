import sys
import os
import logging
from typing import List
from src.helpers import (
    GEOPARQUET_DIR,
    GRID_SIZE,
    TEST_PARQUET_FILE,
    BAND_COLUMN_NAMES,
    OUTPUT_RASTER_FILE,
    BAND16_COLS,
    BAND32_COLS,
)
from src.transform_populate import (
    read_parquet,
    create_band_array,
)
from src.generate_raster import (
    make_raster_profile,
    write_single_band_raster,
    write_multiband_raster_chunks,
)

# Logging setup
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def full_one_band_pipeline(
    parquet_path: str = os.path.join(GEOPARQUET_DIR, TEST_PARQUET_FILE),
    band_column: str = "avg_u_kbps",
) -> None:
    """
    Entire pipeline will read parquet files -> build arrays -> write raster output files
    Available functionalities: make geopackages from parquets
    Incoming functionalities: manually clearing memory storage after array creation and writing to memory
    allowing different dtypes for different columns
    """
    gdf = read_parquet(parquet_path)
    if gdf is None:
        logger.error("GeoDataFrame loading failed. Aborting raster generation.")
        return
    band_array = create_band_array(gdf, band_column)
    profile = make_raster_profile(num_bands=5, grid_size=GRID_SIZE)
    write_single_band_raster(band_array, profile)
    # cleanup memory
    # del gdf, array_data
    # gc.collect()

    logger.info("Raster pipeline complete.")


def full_multiband_ookla_pipeline(
    parquet_path: str = os.path.join(GEOPARQUET_DIR, TEST_PARQUET_FILE),
    band_columns: List[str] = BAND_COLUMN_NAMES,
    output_path: str = OUTPUT_RASTER_FILE,
) -> None:
    """
    Full pipeline: read parquet → generate raster bands → write rasters for all bands.
    """

    # reading the parquet into the gdf
    gdf = read_parquet(parquet_path)
    if gdf is None:
        logger.error("GeoDataFrame loading failed. Aborting raster generation.")
        return
    # writing the profile
    profile = make_raster_profile(num_bands=5, grid_size=GRID_SIZE)
    # writing in the bands in increments (the uint32 stacks)
    write_multiband_raster_chunks(
        gdf,
        band32_cols=BAND32_COLS,
        band16_cols=BAND16_COLS,
        profile=profile,
        output_path=OUTPUT_RASTER_FILE,
    )


if __name__ == "__main__":
    full_multiband_ookla_pipeline()
