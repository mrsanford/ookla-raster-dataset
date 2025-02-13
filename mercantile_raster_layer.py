# Imports
from dataset_download import *
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_origin
import logging
from pathlib import Path
import mercantile

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
ZOOM_LEVEL = 16
GRID_SIZE = 2**ZOOM_LEVEL
BAND_COLUMN_NAME = ["avg_d_kbps", "avg_u_kbps", "avg_lat_ms", "tests", "devices"]
NUM_BAND = 5
geoparquet_dir = Path(
    "/Users/michellesanford/Documents/GitHub/geo-datasets/datasets/ookla_speedtest"
)
test_parquet_file = "2019-01-01_performance_fixed_tiles.parquet"


# Function to Read Parquet Data
def read_parquet(geoparquet_dir, sample_size=1000):
    if geoparquet_dir.exists():
        logger.info(f"Attempting to read Parquet file at: {geoparquet_dir}")
        parquet_data = pd.read_parquet(geoparquet_dir)

        logger.info(f"Columns in the Parquet file: {parquet_data.columns}")
        if "tile" in parquet_data.columns:
            parquet_data = parquet_data.sample(n=sample_size)
            parquet_data["geometry"] = gpd.GeoSeries.from_wkt(parquet_data["tile"])
            gdf = gpd.GeoDataFrame(parquet_data)
            logger.info(
                f"GeoDataFrame successfully loaded. First few rows:\n{gdf.head()}"
            )
            return gdf
        else:
            logger.error("Column 'tile' not found in Parquet file.")
            return None
    else:
        logger.warning(f"Parquet file not found: {geoparquet_dir}")
        return None


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


# Function to create a raster profile
def make_raster_profile(
    gdf: gpd.GeoDataFrame,
    zoom_level: int = ZOOM_LEVEL,
    grid_size: int = GRID_SIZE,
    num_bands: int = NUM_BAND,
) -> dict:
    if not isinstance(gdf, gpd.GeoDataFrame):
        logger.error("Input data is not a GeoDataFrame!")
        return None
    minx, miny, maxx, maxy = gdf.total_bounds
    width = int((maxx - minx) / grid_size)
    height = int((maxy - miny) / grid_size)
    width = max(1, width)
    height = max(1, height)
    transform = from_origin(minx, maxy, grid_size, grid_size)
    profile = {
        "driver": "GTiff",
        "count": num_bands,
        "dtype": "float32",
        "crs": CRS.from_epsg(3857),
        "transform": transform,
        "width": width,
        "height": height,
    }
    logger.info(f"Raster profile: width={width}, height={height}")
    return profile


# Create empty bands
def make_raster_bands(
    grid_size: int = GRID_SIZE, num_bands: int = NUM_BAND
) -> np.ndarray:
    return np.empty((num_bands, grid_size, grid_size))


# Function converts quadkey to XYZ coordinates
def quadkey_to_xyz(quadkey: str, zoom_level: int = ZOOM_LEVEL) -> tuple:
    tile = mercantile.quadkey_to_tile(quadkey)
    logger.info(f"Quadkey: {quadkey} -> Tile coordinates: {tile}")
    # array[tile.x][tile.y]
    # calculate the x_index --> x_index = tile.x-100
    return tile.x, tile.y


# everytime we create a tile, create an assert(tile.zoom=16) to ensure the tiles stay the same zoom


# Process polygon data and assign values to raster bands
def process_polygon_data(
    gdf: gpd.GeoDataFrame,
    grid_size: int = GRID_SIZE,
    band_column_names: list = BAND_COLUMN_NAME,
    zoom_level: int = ZOOM_LEVEL,
) -> np.ndarray:
    minx, miny, maxx, maxy = gdf.total_bounds
    all_bands = make_raster_bands(grid_size, len(band_column_names))
    for idx, row in gdf.iterrows():
        quadkey = row["quadkey"]
        x, y = quadkey_to_xyz(quadkey, zoom_level)

        raster_x = int((x - minx) / grid_size)
        raster_y = int((y - miny) / grid_size)
        if 0 <= raster_x < grid_size and 0 <= raster_y < grid_size:
            for band_idx, band_column in enumerate(band_column_names):
                if band_column in row:
                    all_bands[band_idx, raster_y, raster_x] = row[band_column]
                else:
                    logger.warning(f"Missing data for {band_column} at row {idx}")
    return all_bands


# Write raster to file
def write_raster(all_bands: np.ndarray, profile: dict, output_path: str):
    try:
        with rasterio.open(output_path, "w", **profile, compress="lzw") as dst:
            dst.write(all_bands)
        logger.info(f"Raster written to {output_path}")
    except Exception as e:
        logger.error(f"Error writing raster: {e}")


# Main function to execute the script
def main():
    parquet_data_path = geoparquet_dir / test_parquet_file
    gdf = read_parquet(parquet_data_path, sample_size=1000)
    if gdf is not None:
        profile = make_raster_profile(gdf, ZOOM_LEVEL, GRID_SIZE, NUM_BAND)
        all_bands = process_polygon_data(gdf, GRID_SIZE, BAND_COLUMN_NAME, ZOOM_LEVEL)
        write_raster(all_bands, profile, "ookla_raster.tif")
    else:
        logger.error("GeoDataFrame is empty. Cannot process raster.")


if __name__ == "__main__":
    main()
