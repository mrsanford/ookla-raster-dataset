import os
import numpy as np

# Constants
BUCKET_NAME = "ookla-open-data"
REGION_NAME = "us-west-2"
QUARTERS = {1: "01", 2: "04", 3: "07", 4: "10"}

# Paths (Relative to Project Root)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

GEOPARQUET_DIR = os.path.join(BASE_DIR, "data", "datasets")
RASTER_OUTPUT_DIR = os.path.join(BASE_DIR, "visualizations")

os.makedirs(GEOPARQUET_DIR, exist_ok=True)
os.makedirs(RASTER_OUTPUT_DIR, exist_ok=True)

# Other Constants
TEST_PARQUET_FILE = "2019-01-01_performance_fixed_tiles.parquet"
OUTPUT_RASTER_FILE = os.path.join(
    RASTER_OUTPUT_DIR, TEST_PARQUET_FILE.replace(".parquet", ".tif")
)

# Raster Processing Constants
ZOOM_LEVEL = 16
GRID_SIZE = 2**ZOOM_LEVEL
BAND_COLUMN_NAMES = ["avg_lat_ms", "tests", "devices"]  # ["avg_d_kbps", "avg_u_kbps"]
BAND_DTYPES = {
    # "avg_d_kbps": np.uint32,
    # "avg_u_kbps": np.uint32,
    "avg_lat_ms": np.uint16,
    "tests": np.uint16,
    "devices": np.uint16,
}
NUM_BAND = 3
# For EPSG 3857
MAP_BOUNDS = (
    -20037508.342789244,
    -20037508.342789244,
    20037508.342789244,
    20037508.342789244,
)
