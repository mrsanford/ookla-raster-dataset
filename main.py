from src.helpers import (
    GEOPARQUET_DIR,
    GRID_SIZE,
    TEST_PARQUET_FILE,
    OUTPUT_RASTER_FILE,
)
from src.transform_populate import read_parquet, create_band_array
from src.generate_raster import (
    make_raster_profile,
    write_single_band_raster,
)
import sys
import os
import numpy as np
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def parquet_to_raster(
    parquet_path: str = os.path.join(GEOPARQUET_DIR, TEST_PARQUET_FILE),
    output_path: str = OUTPUT_RASTER_FILE,
) -> None:
    """
    Full pipeline: reads parquet -> builds array -> writes raster -> clears memory
    """
    # 1. Load GeoDataFrame
    parquet_path = os.path.join(GEOPARQUET_DIR, TEST_PARQUET_FILE)
    gdf = read_parquet(parquet_path)
    if gdf is None:
        logger.error("GeoDataFrame loading failed. Aborting raster generation.")
        return
    # building single array
    band_array = create_band_array(gdf, "avg_u_kbps")

    # # 2. Build uint32 array
    # array_data = populate_array(gdf)

    # 3. Create raster profile (uint32-compatible)
    profile = make_raster_profile(num_bands=band_array.shape[0], grid_size=GRID_SIZE)

    # 4. Write the raster to disk
    flipped_array = np.flipud(band_array)
    write_single_band_raster(flipped_array, profile)
    # write_raster(band_array, profile, output_path=output_path)

    # 5. Cleanup memory
    # del gdf, array_data
    # gc.collect()

    logger.info("Raster pipeline complete.")


if __name__ == "__main__":
    parquet_to_raster()
