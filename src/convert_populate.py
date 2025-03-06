# go down to one band
# put everything in np.array
# do some studying on the np.array after the data has loaded
# check the discrepancies between quadkey zoom and mercantile zoom
# how does the quadkey relate to the position in the array
# for this quadkey, go into the fresh np array and for each of the values
# putting their corresponding data values into the correct index
# for quadkeys and their mappings you
# for value, key in quadkeys:
# calculate x, y indices for that quadkey
# array[x][y] = value

# Imports
from src.helpers import (
    GEOPARQUET_DIR,
    ZOOM_LEVEL,
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
from tqdm import tqdm
from pyquadkey2.quadkey import QuadKey
# import matplotlib.pyplot as plt

# Logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def parquet_iterate() -> None:
    for parquet_file in GEOPARQUET_DIR.glob("*.parquet"):
        TEST_PARQUET_FILE = parquet_file
    return None


def read_parquet(GEOPARQUET_DIR):
    if GEOPARQUET_DIR.exists():
        logger.info(f"Reading Parquet file: {GEOPARQUET_DIR}")
        parquet_data = pd.read_parquet(GEOPARQUET_DIR)
        parquet_data["geometry"] = gpd.GeoSeries.from_wkt(parquet_data["tile"])
        gdf = gpd.GeoDataFrame(parquet_data)
        logger.info(parquet_data.head())
        return gdf
    else:
        logger.warning(f"Parquet file not found: {GEOPARQUET_DIR}")
        return None


def convert_quadkey_to_tile(quadkey: str, zoom_level: int = ZOOM_LEVEL) -> tuple:
    quadkey_obj = QuadKey(quadkey)
    x, y = quadkey_obj.tile
    x_idx = x % GRID_SIZE
    y_idx = GRID_SIZE - 1 - (y % GRID_SIZE)
    return x_idx, y_idx


def populate_array(gdf: gpd.GeoDataFrame, band_column_names: list = BAND_COLUMN_NAME):
    array_data = np.full((NUM_BAND, GRID_SIZE, GRID_SIZE), np.nan, dtype=float)
    for idx, row in tqdm(gdf.iterrows(), total=len(gdf)):
        quadkey = row["quadkey"]
        # print(quadkey)
        # logger.debug(f"Processing quadkey: {quadkey}")
        x, y = convert_quadkey_to_tile(quadkey, ZOOM_LEVEL)
        # print((f"Calculated (x, y) = ({x}, {y}) for quadkey: {quadkey}"))
        # logger.debug(f"Calculated (x, y) = ({x}, {y}) for quadkey: {quadkey}")
        for band_idx, band_column in enumerate(band_column_names):
            if band_column in row:
                value = row[band_column]
                logger.debug(f"Putting value {value} to array[{x},{y}]")
                array_data[band_idx, x, y] = value
            else:
                logger.warning(f"Missing data for {band_column} at row {idx}")
                array_data[band_idx, x, y] = np.nan
    return array_data
