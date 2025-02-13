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
from dataset_download import *
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import logging
import rasterio
from rasterio.crs import CRS
from pyquadkey2.quadkey import QuadKey


# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
ZOOM_LEVEL = 16
GRID_SIZE = 2**ZOOM_LEVEL
BAND_COLUMN_NAME = ["avg_d_kbps"]  # "avg_u_kbps", "avg_lat_ms", "tests", "devices"
NUM_BAND = 1
geoparquet_dir = Path(
    "/Users/michellesanford/Documents/GitHub/geo-datasets/datasets/ookla_speedtest"
)
test_parquet_file = "2019-01-01_performance_fixed_tiles.parquet"
# for parquet_file in geoparquet_dir.glob('*.parquet'):
#     test_parquet_file = parquet_file


def read_parquet(geoparquet_dir):
    if geoparquet_dir.exists():
        logger.info(f"Reading Parquet file: {geoparquet_dir}")
        parquet_data = pd.read_parquet(geoparquet_dir)
        parquet_data["geometry"] = gpd.GeoSeries.from_wkt(parquet_data["tile"])
        gdf = gpd.GeoDataFrame(parquet_data)
        logger.info(parquet_data.head())
        return gdf
    else:
        logger.warning(f"Parquet file not found: {geoparquet_dir}")
        return None


def convert_quadkey_to_tile(quadkey: str, zoom_level: int = ZOOM_LEVEL) -> tuple:
    quadkey_obj = QuadKey(quadkey)
    x, y = quadkey_obj.tile
    x_idx = x % GRID_SIZE
    y_idx = GRID_SIZE - 1 - (y % GRID_SIZE)
    return x_idx, y_idx


def populate_array(gdf: gpd.GeoDataFrame, band_column_names: list = BAND_COLUMN_NAME):
    array_data = np.zeros((GRID_SIZE, GRID_SIZE), dtype=float)
    for idx, row in gdf.iterrows():
        quadkey = row["quadkey"]
        logger.debug(f"Processing quadkey: {quadkey}")

        x, y = convert_quadkey_to_tile(quadkey, ZOOM_LEVEL)

        for band_column in band_column_names:
            if band_column in row:
                value = row[band_column]
                array_data[x, y] = value
            else:
                logger.warning(f"Missing data for {band_column} at row {idx}")
                array_data[x, y] = np.nan
    return array_data


def process_polygon_data(
    gdf: gpd.GeoDataFrame, band_column_names: list = BAND_COLUMN_NAME
):
    """Process GeoDataFrame and load data into a numpy array for inspection."""
    loaded_data_array = populate_array(gdf, band_column_names)
    logger.info("Loaded data array:")
    logger.info(loaded_data_array)
    return loaded_data_array


# Optional: makes geopackage
def make_geopackage(gdf: gpd.GeoDataFrame, output_path: Path):
    try:
        if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
            gdf.to_file(output_path, driver="GPKG")
            logger.info(f"Saved data as GeoPackage: {output_path}")
        else:
            logger.warning("Invalid GeoDataFrame provided to make_geopackage.")
    except Exception as e:
        logger.error(f"Failed to save GeoPackage: {e}")


# Raster Making
def make_raster_profile(
    zoom_level: int = ZOOM_LEVEL, grid_size: int = GRID_SIZE, num_bands: int = NUM_BAND
) -> dict:
    profile = {
        "driver": "GTiff",
        "count": num_bands,
        "dtype": "float32",
        "crs": CRS.from_epsg(3857),
        "transform": rasterio.transform.from_origin(
            0, 0, grid_size, grid_size
        ),  # You will need to adjust this for your actual bounding box
        "width": grid_size,
        "height": grid_size,
    }
    return profile


def write_raster(all_bands: np.ndarray, profile: dict, output_path: str):
    try:
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(all_bands)
        logger.info(f"Raster written to {output_path}")
    except Exception as e:
        logger.error(f"Error writing raster: {e}")


def main():
    parquet_data_path = geoparquet_dir / test_parquet_file
    gdf = read_parquet(parquet_data_path)
    if gdf is not None:
        updated_array = populate_array(gdf, BAND_COLUMN_NAME)
        logger.info(f"Updated Array Shape: {updated_array.shape}")

        # Testing some of the array stats
        print(updated_array[:10, :10])
        print(updated_array[1000:1010, 1000:1010])
        print(updated_array[0, :])
        print(updated_array[:, 0])

        # Array Heatmap
        plt.imshow(updated_array, cmap="hot", interpolation="nearest")
        plt.colorbar()
        plt.show()
        logger.info(f"Sample Data at (x=0, y=0): {updated_array[0, 0]}")
    else:
        logger.error("GeoDataFrame is empty. Cannot process raster.")


if __name__ == "__main__":
    main()
