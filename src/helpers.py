import os

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
OUTPUT_FILE = os.path.join(RASTER_OUTPUT_DIR, TEST_PARQUET_FILE)

# Raster Processing Constants
ZOOM_LEVEL = 16
GRID_SIZE = 2**ZOOM_LEVEL
BAND_COLUMN_NAME = ["avg_u_kbps"]
# Options: "avg_lat_ms", "tests", "devices", "avg_d_kbps"
NUM_BAND = 1
