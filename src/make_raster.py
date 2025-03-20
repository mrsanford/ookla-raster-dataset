from src.helpers import GRID_SIZE, NUM_BAND, OUTPUT_FILE, MAP_BOUNDS
from rasterio.transform import from_bounds
from rasterio.crs import CRS
import rasterio
import numpy as np
import logging
import sys
from typing import Dict, Tuple

# instantiating the logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


# Raster Making
def make_raster_profile(
    grid_size: int = GRID_SIZE,
    num_bands: int = NUM_BAND,
    bounds: Tuple[float, float, float, float] = MAP_BOUNDS,
) -> Dict[str, object]:
    """
    Creates the raster profiile (metadata dictionary) for writing the GeoTIFF raster file
        Note: the tiles need to stretch across the entire bounds; the tiff needs to fit
        the entire bounds of the coordinate reference system
        i.e. rasterio needs to be told that a 4x4 raster size ≠ 4x4 coordinates in the crs space
    ---
    Args:
        grid_size (int) is the size of the raster grid (width x height) and default is based
            on ZOOM_SIZE = 16
        num_bands (int) is the number of bands (layers) in the raster
    Returns:
        Dict[str, object]
    """
    left, bottom, right, top = bounds
    transform = from_bounds(left, bottom, right, top, grid_size, grid_size)
    profile = {
        "driver": "GTiff",
        "count": num_bands,
        "dtype": "float32",
        "crs": CRS.from_epsg(3857),
        "transform": transform,
        "width": grid_size,
        "height": grid_size,
    }
    return profile


def write_raster(
    all_bands: np.ndarray, profile: dict, output_path: str = OUTPUT_FILE
) -> None:
    """
    Writes the 3D numpy array (with the # of raster bands) to a GeoTIFF using rasterio
    ----
    Args:
        all_bands (np.ndarray) is the 3D array; size is (num_bands, height, width)
        profile (Dict[str,object]) is the raster profile
        output_path (str) is the path for the output raster file
    Returns None
    """
    try:
        if len(all_bands.shape) != 3:
            logger.warning(
                f"Expected 3D array (bands, height, width), but got shape {all_bands.shape}."
            )
            all_bands = np.expand_dims(all_bands, axis=0)
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(all_bands)
        logger.info(f"Raster written to {output_path}")
    except Exception as e:
        logger.error(f"Error writing raster: {e}")
