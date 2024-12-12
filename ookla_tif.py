# Extra Questions
## I have a script that converts the geoparquet to a geopackage, which can be opened in QGIS, but other than the graphical benefit, is there a benefit to this code?

# imports
import geopandas as gpd
import numpy as np
from pathlib import Path
import rasterio
# importing function from my other file
from boto3process2 import read_parquet
from pyquadkey2 import QuadKey
from affine import Affine
from rasterio.crs import CRS

# reading in data
parquet_data_path = Path('/Users/michellesanford/GitHub/geo-datasets/datasets/ookla_speedtest/2019-01-01_performance_fixed_tiles.parquet')
parquet_data_path = read_parquet(parquet_data_path)
# checking data
print(parquet_data_path.head())
print(parquet_data_path.columns)

# setting zoom level and grid size
zoom_level = 16
grid_size = 2 ** zoom_level
band_column_names = ['avg_d_kbps','avg_u_kbps','avg_lat_ms','tests','devices']
num_bands = len(band_column_names)

# creating the empty band arrays
d_kbps_band = np.empty((grid_size,grid_size))
u_kbps_band = np.empty((grid_size,grid_size))
lat_ms_band = np.empty((grid_size,grid_size))
tests_band = np.empty((grid_size,grid_size))
devices_band = np.empty((grid_size,grid_size))
all_bands = np.stack([d_kbps_band,u_kbps_band,lat_ms_band,tests_band,devices_band],axis=0)

# # iterating over the rows
# for idx, row in parquet_data_path.iterrows():
#     quadkey = row['quadkey']
#     x, y = QuadKey.decode(quadkey, zoom_level)
#     if 0 <= x < grid_size and 0 <= y < grid_size:
#         for band_idx, band_column in enumerate(band_column_names):
#             if band_column in row:
#                 all_bands[band_idx, x, y] = row[band_column]
#                 # band_arrays[band_idx][x, y] = row[band_column]
#             else:
#                 print(f"Missing data for {band_column} at row {idx}")
# print(all_bands[:, :5, :5])

profile = {
    'driver': 'GTiff',
    'count': num_bands,
    'dtype': 'float32',
    'crs': CRS.from_epsg(3857),
    'transform': rasterio.transform.from_origin(0, grid_size, 1, 1),  # You will need to adjust this for your actual bounding box
    'width': grid_size,
    'height': grid_size,
}
with rasterio.open('ookla_raster.tif','w,',**profile) as dst:
    dst.write(all_bands)