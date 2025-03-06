from src.helpers import (
    GEOPARQUET_DIR,
    GRID_SIZE,
    BAND_COLUMN_NAME,
    NUM_BAND,
    TEST_PARQUET_FILE,
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


def parquet_iterate(file_name: str = TEST_PARQUET_FILE) -> None:
    for parquet_file in GEOPARQUET_DIR.glob("*.parquet"):
        logger.info(f"Processing file: {parquet_file}")
        # Logic for reading parquet_file
    return None


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
        logger.info(parquet_data.head())
        return gdf
    else:
        logger.warning(f"Parquet file not found: {parquet_file}")
        return None


def quadkey_to_tile(quadkey: str) -> tuple:
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


def populate_array(
    gdf: gpd.GeoDataFrame, band_column_names: List[str] = BAND_COLUMN_NAME
) -> np.ndarray:
    """
    Populates a 3D numpy array with the band data from the GeoDataFrame.
    The spatial positions within the array have been determined by the quadkeys.
    ----
    Args:
    gdf (gpd.GeoDataFrame) is the GeoDataFrame with the spatial data per band
    band_column_names (List[str]) is the list of column names representing bands to extract
    Returns:
    np.ndarray of the shape (NUM_BAND, GRID_SIZE, GRID_SIZE) placed based on the
    spatial positions (via quadkey)
    """
    array_data = np.full((NUM_BAND, GRID_SIZE, GRID_SIZE), np.nan, dtype=float)
    for idx, row in tqdm(gdf.iterrows(), total=len(gdf)):
        try:
            quadkey = row["quadkey"]
            x, y = quadkey_to_tile(quadkey)
            for band_idx, band_column in enumerate(band_column_names):
                value = row.get(band_column, np.nan)
                array_data[band_idx, x, y] = value
        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}")
    return array_data
