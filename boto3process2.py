# Instructions
# check geoparquet, see if there is a geopandas in a parquet --> but it didn't work because it said there was no geospatial metadata
# save as a geopackage if geoparquet has issues
# load geopackage with geopandas
# start looking at each of the polygons
from boto3ingest import *
import geopandas as gpd
import pandas as pd
import logging
from shapely import wkt
from pathlib import Path
from shapely.wkt import loads

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

geoparquet_dir = Path('/Users/michellesanford/GitHub/geo-datasets/datasets/ookla_speedtest')
test_parquet_file = '2019-01-01_performance_fixed_tiles.parquet'

def read_parquet(parquet_path: str):
    if parquet_path.exists():
        logger.info(f'Reading Parquet file: {parquet_path}')
        parquet_data = pd.read_parquet(parquet_path)
        parquet_data["geometry"] = gpd.GeoSeries.from_wkt(parquet_data["tile"])
        gdf = gpd.GeoDataFrame(parquet_data)
        logger.info(parquet_data.head())
        return gdf
    else:
        logger.warning(f'Parquet file not found: {parquet_path}')
        return None

def make_geopackage(gdf, output_path):
    if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
        try:
            gdf.to_file(output_path, driver='GPKG')
            logger.info(f'Saved data as GeoPackage: {output_path}')
        except Exception as e:
            logger.error(f'Failed to save GeoPackage: {e}')
    else:
        logger.warning('Invalid GeoDataFrame provided to make_geopackage.')

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
    process_geodata()

# additional considerations would be creating a loop to get all of the .gpkgs created