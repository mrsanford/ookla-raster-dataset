from src.helpers import GRID_SIZE, NUM_BAND, OUTPUT_RASTER_FILE, MAP_BOUNDS
from rasterio.transform import from_bounds
from rasterio.crs import CRS
import rasterio
import numpy as np
import logging
import sys
from typing import Dict

# instantiating the logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def make_raster_profile(
    num_bands: int = NUM_BAND, grid_size: int = GRID_SIZE
) -> Dict[str, object]:
    """
    Creates the profile for GeoTIFF raster file, and ensures the
    raster stretches across entire EPSG:3857 coordinate space
    ---
    Args:
        grid_size (int): raster grid size (width x height)
        num_bands (int): number of raster bands
    Returns:
        Dict[str, object]: raster metadata profile
    """
    left, bottom, right, top = MAP_BOUNDS
    transform = from_bounds(left, bottom, right, top, grid_size, grid_size)
    profile = {
        "driver": "GTiff",
        "count": num_bands,
        "dtype": "uint32",
        "crs": CRS.from_epsg(3857),
        "transform": transform,
        "width": grid_size,
        "height": grid_size,
    }
    return profile


def write_single_band_raster(
    band_array: np.ndarray, profile: dict, output_path: str = OUTPUT_RASTER_FILE
) -> None:
    """
    Writing single 2D band array to a GeoTIFF
    """
    try:
        if band_array.ndim != 2:
            logger.error(
                f"Expected 2D array for single band, got shape {band_array.shape}"
            )
            return
        profile = profile.copy()
        profile["count"] = 1
        profile["dtype"] = str(band_array.dtype)
        profile["BIGTIFF"] = "YES"
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(band_array, 1)
        logger.info(f"Single-band raster written to {output_path}")
    except Exception as e:
        logger.error(f"Error writing single-band raster: {e}")
    return


def write_multiband_raster(
    all_bands: np.ndarray, profile: dict, output_path: str = OUTPUT_RASTER_FILE
) -> None:
    """
    Writes the mapped 3D numpy array to a single multiband GeoTIFF
    """
    try:
        if all_bands.ndim != 3:
            logger.warning(
                f"Expected 3D array (bands, height, width), got {all_bands.shape}"
            )
            all_bands = np.expand_dims(all_bands, axis=0)
        # flips vertically to match geospatial top-left origin
        all_bands = np.flip(all_bands, axis=1)
        breakpoint()
        profile = profile.copy()
        profile["count"] = all_bands.shape[0]
        profile["dtype"] = str(all_bands.dtype)
        profile["BIGTIFF"] = "YES"
        breakpoint()
        with rasterio.open(output_path, "w", **profile) as dst:
            breakpoint()
            dst.write(all_bands)
            breakpoint()
        logger.info(
            f"Multi-band raster written to {output_path} with shape {all_bands.shape}"
        )
    except Exception as e:
        logger.error(f"Error writing multi-band raster: {e}")
    return
