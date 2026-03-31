from pathlib import Path

# Constants
BUCKET_NAME = "ookla-open-data"
REGION_NAME = "us-west-2"
QUARTERS = {1: "01", 2: "04", 3: "07", 4: "10"}

# Paths (Relative to Project Root)
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
GEOPARQUET_DIR = DATA_DIR / "datasets"
RASTER_OUTPUT_DIR = BASE_DIR / "visualizations"

# Ensure Directory Existence
GEOPARQUET_DIR.mkdir(parents=True, exist_ok=True)
RASTER_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Raster Processing Constants
ZOOM_LEVEL = 16
GRID_SIZE = 2**ZOOM_LEVEL
BAND32_COLS = ["avg_d_kbps", "avg_u_kbps"]
BAND16_COLS = ["avg_lat_ms", "tests", "devices"]
NUM_BANDS = 5

# For EPSG 3857
MAP_BOUNDS = (
    -20037508.342789244,
    -20037508.342789244,
    20037508.342789244,
    20037508.342789244,
)

# OS Version
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# GEOPARQUET_DIR = os.path.join(BASE_DIR, "data", "datasets")
# RASTER_OUTPUT_DIR = os.path.join(BASE_DIR, "visualizations")
# os.makedirs(GEOPARQUET_DIR, exist_ok=True)
# os.makedirs(RASTER_OUTPUT_DIR, exist_ok=True)
