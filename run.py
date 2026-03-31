from pathlib import Path
from typing import List
from utils.helpers import (
    GEOPARQUET_DIR,
    RASTER_OUTPUT_DIR,
    GRID_SIZE,
    NUM_BANDS,
    BAND16_COLS,
    BAND32_COLS,
)
from utils.loggers import setup_custom_logger
from src.transform_populate import read_parquet
from src.generate_raster import make_raster_profile, write_multiband_raster_chunks


logger = setup_custom_logger("EXECUTE_PIPELINE")


def run_pipeline(
    parquet_path: Path,
    output_path: Path,
    grid_size: int = GRID_SIZE,
    num_bands: int = NUM_BANDS,
    band32_cols: List[str] = BAND32_COLS,
    band16_cols: List[str] = BAND16_COLS,
):
    """
    Processes a SINGLE file using Path objects.
    """
    logger.info(f"--- Starting: {parquet_path.name} ---")
    gdf = read_parquet(parquet_path)
    if gdf is None:
        return
    profile = make_raster_profile(num_bands=num_bands, grid_size=grid_size)
    write_multiband_raster_chunks(
        gdf,
        band32_cols=band32_cols,
        band16_cols=band16_cols,
        profile=profile,
        output_path=str(output_path),
    )
    logger.info(f"Successfully saved to: {output_path}")


def run_batch_process():
    """
    Orchestrates the batch iteration and naming conventions.
    """
    files_to_process = list(GEOPARQUET_DIR.glob("*.parquet"))
    if not files_to_process:
        logger.warning(f"No parquet files found in {GEOPARQUET_DIR}")
        return
    logger.info(f"Found {len(files_to_process)} files. Starting batch...")
    for input_path in files_to_process:
        output_name = input_path.with_suffix(".tif").name
        output_path = RASTER_OUTPUT_DIR / output_name
        run_pipeline(parquet_path=input_path, output_path=output_path)


if __name__ == "__main__":
    run_batch_process()
