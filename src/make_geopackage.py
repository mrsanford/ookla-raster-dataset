import geopandas as gpd
from pathlib import Path
import logging
import sys

# instantiating the logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def make_geopackage(gdf: gpd.GeoDataFrame, output_path: Path):
    try:
        if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
            gdf.to_file(output_path, driver="GPKG")
            logger.info(f"Saved data as GeoPackage: {output_path}")
        else:
            logger.warning("Invalid GeoDataFrame provided to make_geopackage.")
    except Exception as e:
        logger.error(f"Failed to save GeoPackage: {e}")
