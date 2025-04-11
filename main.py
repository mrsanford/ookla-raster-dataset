import os
from typing import List
from src.helpers import (
    GEOPARQUET_DIR,
    TEST_PARQUET_FILE,
    GRID_SIZE,
    NUM_BANDS,
    BAND16_COLS,
    BAND32_COLS,
    OUTPUT_RASTER_FILE,
)
from src.transform_populate import read_parquet
from src.generate_raster import make_raster_profile, write_multiband_raster_chunks


def full_multiband_ookla_pipeline(
    parquet_path: str = os.path.join(GEOPARQUET_DIR, TEST_PARQUET_FILE),
    grid_size: int = GRID_SIZE,
    num_bands: int = NUM_BANDS,
    band32_cols: List[int] = BAND32_COLS,
    band16_cols: List[int] = BAND16_COLS,
    output_path: str = OUTPUT_RASTER_FILE,
):
    """
    Full pipeline: read parquet → generate raster bands → write rasters for all bands.
    """
    # reading the parquet into the gdf
    gdf = read_parquet(parquet_path)

    # writing the profile
    profile = make_raster_profile(num_bands=num_bands, grid_size=grid_size)
    # writing in the bands in increments (the uint32 stacks)
    write_multiband_raster_chunks(
        gdf,
        band32_cols=band32_cols,
        band16_cols=band16_cols,
        profile=profile,
        output_path=output_path,
    )


if __name__ == "__main__":
    full_multiband_ookla_pipeline()
