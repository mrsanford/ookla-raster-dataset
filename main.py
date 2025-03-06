from src.helpers import GEOPARQUET_DIR, TEST_PARQUET_FILE, BAND_COLUMN_NAME
from src.convert_populate import read_parquet, populate_array
from src.make_raster import make_raster_profile, write_raster
import logging
import sys

# Logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def main():
    parquet_data_path = GEOPARQUET_DIR / TEST_PARQUET_FILE
    gdf = read_parquet(parquet_data_path)
    # checking column values
    # print(gdf.columns)
    # d_kbps = gdf.get("avg_d_kbps")
    # print(d_kbps)
    if gdf is not None:
        updated_array = populate_array(gdf, BAND_COLUMN_NAME)
        # Testing some of the array stats
        print(updated_array[15540:15550, 40450:40460])
        # Making the raster
        profile = make_raster_profile()
        raster_path_out = "/Users/michellesanford/Documents/GitHub/geo-datasets/datasets/ookla_raster_output.tif"
        write_raster(updated_array, profile, raster_path_out)
        # logger.info(f"Sample Data at (x=0, y=0): {updated_array[0, 0]}")
    else:
        logger.error("GeoDataFrame is empty. Cannot process raster.")


if __name__ == "__main__":
    main()
