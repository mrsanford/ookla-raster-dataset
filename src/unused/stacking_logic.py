import geopandas as gpd
import numpy as np
import logging

BAND_COLUMN_NAMES = ["avg_lat_ms", "tests", "devices"]  # ["avg_d_kbps", "avg_u_kbps"]
BAND_DTYPES = {
    # "avg_d_kbps": np.uint32,
    # "avg_u_kbps": np.uint32,
    "avg_lat_ms": np.uint16,
    "tests": np.uint16,
    "devices": np.uint16,
}


#  def stack_band_arrays(
#     gdf: gpd.GeoDataFrame,
#     band_columns: List[str] = BAND_COLUMN_NAMES,
#     dtype_map: dict = BAND_DTYPES,
# ) -> np.ndarray:
#     """
#     Stacks individual band arrays into a 3D array
#     ----
#     Args:
#         gdf (GeoDataFrame): input geospatial data
#         band_columns (List[str]): list of columns to turn into bands
#     Returns:
#         np.ndarray: stacked array of shape (NUM_BAND, GRID_SIZE, GRID_SIZE)
#     """
#     band_arrays = []
#     for column in band_columns:
#         logger.info(f"Creating band for column: {column}")
#         dtype = dtype_map.get(column, np.uint16)
#         band_array = create_band_array(gdf, column, dtype=dtype)
#         band_arrays.append(band_array)
#         logger.info(f"{column} has been appended")
#     return np.stack(band_arrays, axis=0)


def stack_uint16_bands(gdf: gpd.GeoDataFrame) -> np.ndarray:
    """
    Stacks bands with uint16 dtype into a 3D array.
    """
    uint16_columns = ["avg_lat_ms", "tests", "devices"]
    band_arrays = []
    for column in uint16_columns:
        logger.info(f"Creating uint16 band for column: {column}")
        band_array = create_band_array(gdf, column, dtype=np.uint16)
        band_arrays.append(band_array)
    return np.stack(band_arrays, axis=0)


def stack_uint32_bands(gdf: gpd.GeoDataFrame) -> np.ndarray:
    """
    Stacks bands with uint32 dtype into a 3D array.
    """
    uint32_columns = ["avg_d_kbps", "avg_u_kbps"]
    band_arrays = []
    for column in uint32_columns:
        logger.info(f"Creating uint32 band for column: {column}")
        band_array = create_band_array(gdf, column, dtype=np.uint32)
        band_arrays.append(band_array)
    return np.stack(band_arrays, axis=0)


def stack_all_bands(gdf: gpd.GeoDataFrame) -> np.ndarray:
    """
    Stacks all band arrays from both uint16 and uint32 into a single 3D array.
    """
    uint16_stack = stack_uint16_bands(gdf)
    uint32_stack = stack_uint32_bands(gdf)
    logger.info(
        f"uint16_stack shape: {uint16_stack.shape}, dtype: {uint16_stack.dtype}"
    )
    logger.info(
        f"uint32_stack shape: {uint32_stack.shape}, dtype: {uint32_stack.dtype}"
    )
    logger.info("About to stack all bands...")
    return np.concatenate([uint32_stack, uint16_stack], axis=0)
