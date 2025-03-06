from src.helpers import ZOOM_LEVEL, GRID_SIZE, NUM_BAND
from rasterio.crs import CRS
import rasterio
import numpy as np
import logging
import sys

# instantiating the logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


# Raster Making
def make_raster_profile(
    zoom_level: int = ZOOM_LEVEL, grid_size: int = GRID_SIZE, num_bands: int = NUM_BAND
) -> dict:
    profile = {
        "driver": "GTiff",
        "count": num_bands,
        "dtype": "float32",
        "crs": CRS.from_epsg(3857),
        "transform": rasterio.transform.from_origin(0, 0, grid_size, grid_size),
        "width": grid_size,
        "height": grid_size,
    }
    return profile


def write_raster(all_bands: np.ndarray, profile: dict, output_path: str):
    try:
        if len(all_bands.shape) == 10:
            all_bands = np.expand_dims(all_bands, axis=0)
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(all_bands)
        logger.info(f"Raster written to {output_path}")
    except Exception as e:
        logger.error(f"Error writing raster: {e}")
