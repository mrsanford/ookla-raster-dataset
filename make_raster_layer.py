# imports
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

geoparquet_dir = Path('/Users/michellesanford/GitHub/geo-datasets/datasets/ookla_speedtest')
# creating the iterative loop for the files
test_parquet_file = '2019-01-01_performance_fixed_tiles.parquet'
# for parquet_file in geoparquet_dir.glob('*.parquet'):
#     test_parquet_file = parquet_file

# reading the parquet files
def read_parquet(geoparquet_dir):
    if geoparquet_dir.exists():
        logger.info(f'Reading Parquet file: {geoparquet_dir}')
        parquet_data = pd.read_parquet(geoparquet_dir)
        parquet_data["geometry"] = gpd.GeoSeries.from_wkt(parquet_data["tile"])
        gdf = gpd.GeoDataFrame(parquet_data)
        logger.info(parquet_data.head())
        return gdf
    else:
        logger.warning(f'Parquet file not found: {geoparquet_dir}')
        return None
# option to make gpkg files
def make_geopackage(gdf, output_path):
    if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
        try:
            gdf.to_file(output_path, driver='GPKG')
            logger.info(f'Saved data as GeoPackage: {output_path}')
        except Exception as e:
            logger.error(f'Failed to save GeoPackage: {e}')
    else:
        logger.warning('Invalid GeoDataFrame provided to make_geopackage.')
# processing the parquet to gpkg file change
def process_geodata():
    parquet_path = geoparquet_dir / test_parquet_file
    gdf = read_parquet(parquet_path)
    if gdf is not None:
        output_path = geoparquet_dir / '2019-01-01_performance_fixed_tiles.gpkg'
        print('hi')
        make_geopackage(gdf, output_path)
    else:
        logger.error('Failed to read Parquet data or create GeoDataFrame.')
if __name__ == "__main__":
    read_parquet(Path(geoparquet_dir/test_parquet_file))

# reading in data
parquet_data_path = Path(f'/Users/michellesanford/GitHub/geo-datasets/datasets/ookla_speedtest/{test_parquet_file}')
parquet_data_path = read_parquet(parquet_data_path)
# checking data
# print(parquet_data_path.head())
# print(parquet_data_path.columns)

# setting zoom level and grid size
zoom_level = 16
grid_size = 2 ** zoom_level
band_column_names = ['avg_d_kbps','avg_u_kbps','avg_lat_ms','tests','devices']
num_bands = 2
# len(band_column_names)

profile = {
    'driver': 'GTiff',
    'count': num_bands,
    'dtype': 'float32',
    'crs': CRS.from_epsg(3857),
    'transform': rasterio.transform.from_origin(0, grid_size, 1, 1),  # You will need to adjust this for your actual bounding box
    'width': grid_size,
    'height': grid_size}

# creating the empty band arrays
d_kbps_band = np.empty((grid_size,grid_size))
u_kbps_band = np.empty((grid_size,grid_size))
# lat_ms_band = np.empty((grid_size,grid_size))
# tests_band = np.empty((grid_size,grid_size))
# devices_band = np.empty((grid_size,grid_size))
# lat_ms_band,tests_band,devices_band
all_bands = np.stack([d_kbps_band,u_kbps_band],axis=0)

# iterating over the rows
for idx, row in parquet_data_path.iterrows():
    quadkey = row['quadkey']
    x, y = QuadKey.decode(quadkey, zoom_level)
    if 0 <= x < grid_size and 0 <= y < grid_size:
        for band_idx, band_column in enumerate(band_column_names):
            if band_column in row:
                all_bands[band_idx, x, y] = row[band_column]
            else:
                print(f"Missing data for {band_column} at row {idx}")
print(all_bands[:, :2, :2])
with rasterio.open('ookla_raster.tif','w,',**profile) as dst:
    dst.write(all_bands)