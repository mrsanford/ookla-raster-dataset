# Contains paths, constants, and any decorators

# establishing values and directories
BUCKET_NAME = "ookla-open-data"
REGION_NAME = "us-west-2"
LOCAL_DIRECTORY = "/Users/michellesanford/GitHub/geo-datasets/datasets/ookla_speedtest"
QUARTERS = {1: "01", 2: "04", 3: "07", 4: "10"}
GEOPARQUET_DIR = (
    "/Users/michellesanford/Documents/GitHub/geo-datasets/datasets/ookla_speedtest"
)
RASTER_OUTPUT_DIR = (
    "/Users/michellesanford/Documents/GitHub/geo-datasets/datasets/ookla_raster_output"
)
TEST_PARQUET_FILE = "2019-01-01_performance_fixed_tiles.parquet"
OUTPUT_FILE = "./visualizations"

# Constants
ZOOM_LEVEL = 16
GRID_SIZE = 2**ZOOM_LEVEL
BAND_COLUMN_NAME = ["avg_d_kbps"]  # "avg_u_kbps"  "avg_lat_ms", "tests", "devices"]
NUM_BAND = 1
