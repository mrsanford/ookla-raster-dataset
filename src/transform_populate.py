from src.helpers import (
    GEOPARQUET_DIR,
    GRID_SIZE,
    BAND_COLUMN_NAMES,
    BAND_DTYPES,
)
import numpy as np
import pandas as pd
import geopandas as gpd
import logging
import sys
from pathlib import Path
from tqdm import tqdm
from pyquadkey2.quadkey import QuadKey
from typing import List

# Logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def iterate_parquet_files() -> List[Path]:
    return list(GEOPARQUET_DIR.glob("*.parquet"))


def read_parquet(parquet_file: str) -> gpd.GeoDataFrame:
    """
    Reads the Parquet file and converts the 'tile' column into a geometry column
    and creates a GeoDataFrame
    ----
    Args: parquet_file (str) is the path to the parquet file
    Returns: gpd.GeoDataFrame or nothing if the parquet file isn't found
    """
    if Path(parquet_file).exists():
        logger.info(f"Reading Parquet file: {parquet_file}")
        parquet_data = pd.read_parquet(parquet_file)
        parquet_data["geometry"] = gpd.GeoSeries.from_wkt(parquet_data["tile"])
        gdf = gpd.GeoDataFrame(parquet_data)
        logger.info(gdf.head())
        return gdf
    else:
        logger.warning(f"Parquet file not found: {parquet_file}")
        return None


def quadkey_to_tile(quadkey: str) -> tuple[int, int]:
    """
    Converts a quadkey to grid coordinates (x,y) adjusted based on GRID_SIZE
    ----
    Args: quadkey (str) is the quadkey to convert
    Returns: tuple[int, int] represent the (x,y) coordinates within the grid
    """
    quadkey_obj = QuadKey(quadkey)
    x, y = quadkey_obj.tile
    x_idx = x % GRID_SIZE
    y_idx = GRID_SIZE - 1 - (y % GRID_SIZE)
    return x_idx, y_idx


def create_band_array(
    gdf: gpd.GeoDataFrame, band_column: str, dtype=np.float32
) -> np.ndarray:
    """
    Creates a single band array for a given column name with respective data from the DataFrame
    ----
    Args:
        gdf (GeoDataFrame): input geospatial data
        band_column (str): column name to populate the band
    Returns:
        np.ndarray: 2D array of shape (GRID_SIZE, GRID_SIZE)
    """
    band_array = np.full((GRID_SIZE, GRID_SIZE), 0, dtype=dtype)
    for idx in range(len(gdf)):  # opted out of tqdm()
        try:
            row = gdf.iloc[idx]
            quadkey = row["quadkey"]
            x, y = quadkey_to_tile(quadkey)
            value = row.get(band_column, np.nan)
            band_array[y, x] = value
        except Exception as e:
            logger.error(f"Error processing row {idx} for band '{band_column}': {e}")
    logger.info(f"Success loading {band_column} data into array")
    return band_array


# fit most of the values in 16 or 32 bit integer
# potentially make separate np.arrays for each band and then just overlay them later
## we can pick which size of integer for each
# opt out of user functionality for picking band size or whichever
# be able to write a single array to the raster and be able to delete the array
# if done with variable del() command will delete from memory; if no longer using, will delete manually/force delete i.e. geodataframe after loading


def stack_band_arrays(
    gdf: gpd.GeoDataFrame,
    band_columns: List[str] = BAND_COLUMN_NAMES,
    dtype_map: dict = BAND_DTYPES,
) -> np.ndarray:
    """
    Stacks individual band arrays into a 3D array
    ----
    Args:
        gdf (GeoDataFrame): input geospatial data
        band_columns (List[str]): list of columns to turn into bands
    Returns:
        np.ndarray: stacked array of shape (NUM_BAND, GRID_SIZE, GRID_SIZE)
    """
    band_arrays = []
    for column in band_columns:
        logger.info(f"Creating band for column: {column}")
        dtype = dtype_map.get(column, np.uint16)
        band_array = create_band_array(gdf, column, dtype=dtype)
        band_arrays.append(band_array)
        logger.info(f"{column} has been appended")
    return np.stack(band_arrays, axis=0)
