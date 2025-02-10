# Imports
from dataset_download import *
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.wkt import loads
import rasterio
from rasterio.crs import CRS
from pyquadkey2.quadkey import QuadKey
from affine import Affine
import logging
from pathlib import Path
import math

# go down to one band
# put everything in np.array
# do some studying on the np.arraya after the data has loaded
# check the discrepancies between quadkey zoom and mercantile zoom
# how does the quadkey relate to the position in the array
# for this quadkey, go into the fresh np array and for each of the values
# putting their corresponding data values into the correct index
# for quadkeys and their mappings you
# for value, key in quadkeys:
# calculate x, y indices for that quadkey
# array[x][y] = value

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
ZOOM_LEVEL = 16
GRID_SIZE = 2**ZOOM_LEVEL
BAND_COLUMN_NAME = ["avg_d_kbps", "avg_u_kbps", "avg_lat_ms", "tests", "devices"]
NUM_BAND = 5
geoparquet_dir = Path(
    "/Users/michellesanford/GitHub/geo-datasets/datasets/ookla_speedtest"
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


def make_raster_bands(
    grid_size: int = GRID_SIZE, num_bands: int = NUM_BAND
) -> np.ndarray:
    return np.empty((num_bands, grid_size, grid_size))


# its going quadkey to coordinate btw and not the other way around
def coords_to_tile(lat: float, lon: float, zoom_level: int = ZOOM_LEVEL) -> tuple:
    n = GRID_SIZE
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int(
        (
            1.0
            - math.log(math.tan(math.radians(lat)) + 1.0 / math.cos(math.radians(lat)))
            / math.pi
        )
        / 2.0
        * n
    )
    return xtile, ytile


def process_polygon_data(
    gdf: gpd.GeoDataFrame,
    grid_size: int = GRID_SIZE,
    band_column_names: list = BAND_COLUMN_NAME,
    zoom_level: int = ZOOM_LEVEL,
) -> np.ndarray:
    all_bands = make_raster_bands(grid_size, len(band_column_names))
    for idx, row in gdf.iterrows():
        quadkey = row["quadkey"]
        x, y = QuadKey.to_tile(quadkey)  # removed hyperparameter zoom_level
        if 0 <= x < grid_size and 0 <= y < grid_size:
            for band_idx, band_column in enumerate(band_column_names):
                if band_column in row:
                    all_bands[band_idx, x, y] = row[band_column]
                else:
                    logger.warning(f"Missing data for {band_column} at row {idx}")
    return all_bands


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
        profile = make_raster_profile(ZOOM_LEVEL, GRID_SIZE, NUM_BAND)
        all_bands = process_polygon_data(gdf, GRID_SIZE, BAND_COLUMN_NAME, ZOOM_LEVEL)
        write_raster(all_bands, profile, "ookla_raster.tif")
    else:
        logger.error("GeoDataFrame is empty. Cannot process raster.")


if __name__ == "__main__":
    main()
