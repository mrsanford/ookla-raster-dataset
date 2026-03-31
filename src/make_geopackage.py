import geopandas as gpd
from pathlib import Path
from utils.loggers import setup_custom_logger

logger = setup_custom_logger("GEOPACKAGE")


def make_geopackage(gdf: gpd.GeoDataFrame, output_path: str):
    """
    Saves a GeoDataFrame as a GeoPackage (.gpkg)
    Note: this is not a crucial step to obtaining raster output
    and is just an option for additional visualization functionality
    """
    try:
        out_path = Path(output_path)
        if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
            out_path.parent.mkdir(parents=True, exist_ok=True)
            gdf.to_file(out_path, driver="GPKG")
            logger.info(f"Saved data as GeoPackage: {out_path}")
        else:
            logger.warning("Invalid GeoDataFrame provided to make_geopackage.")
    except Exception as e:
        logger.error(f"Failed to save GeoPackage: {e}")
