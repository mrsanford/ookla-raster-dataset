from src.helpers import GRID_SIZE, NUM_BAND, OUTPUT_FILE, MAP_BOUNDS
from rasterio.transform import Affine, from_bounds
from rasterio.crs import CRS
import mercantile
import rasterio
import numpy as np
import logging
import sys
from typing import Dict

# instantiating the logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def transform_calc(
    tile_x: int, tile_y: int, zoom: int, grid_size: int = GRID_SIZE
) -> Affine:
    """
    Use an Affine transformation matrix and snaps the raster to Web Mercator tile boundaries.
    ---
    Args:
        tile_x (int): tile x-coordinate
        tile_y (int): tile y-coordinate
        zoom (int): zoom level
        grid_size (int): raster grid size (width x height)
    Returns:
        Affine: the Affine transformation matrix
    """
    left, bottom, right, top = mercantile.xy_bounds(tile_x, tile_y, zoom)
    pixel_size_x = (right - left) / grid_size
    pixel_size_y = (top - bottom) / grid_size
    # ensures raster starts at the correct tile origin
    transform = Affine.translation(left, top) * Affine.scale(
        pixel_size_x, -pixel_size_y
    )
    return transform


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

    # Stretch raster to fit entire Web Mercator bounds
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
