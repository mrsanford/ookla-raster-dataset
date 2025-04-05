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
    Creates the raster profile for writing a GeoTIFF raster file.
    This ensures the raster stretches across the entire EPSG:3857 coordinate space.
    ---
    Args:
        grid_size (int): Raster grid size (width x height).
        num_bands (int): Number of raster bands.
    Returns:
        Dict[str, object]: Raster metadata profile.
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
    Writes a single 2D band array (65536x65536 max) to a GeoTIFF.
    """
    try:
        if band_array.ndim != 2:
            logger.error(
                f"Expected 2D array for single band, got shape {band_array.shape}"
            )
            return

        profile = profile.copy()
        profile["count"] = 1  # Only one band
        profile["dtype"] = str(band_array.dtype)
        profile["BIGTIFF"] = "YES"  # REQUIRED for large files

        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(band_array, 1)  # Write to band 1

        logger.info(f"Single-band raster written to {output_path}")

    except Exception as e:
        logger.error(f"Error writing single-band raster: {e}")


def write_raster(
    all_bands: np.ndarray, profile: dict, output_path: str = OUTPUT_RASTER_FILE
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
