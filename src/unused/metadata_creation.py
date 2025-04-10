import json
from typing import List


def write_metadata(
    output_path: str,
    band_names: List[str],
    array_shape: tuple,
    dtype: str,
    crs: str,
    transform: tuple,
    extra_metadata: dict = None,
) -> None:
    """
    Writes the JSON metadata files with band names, dtype, CRS, and transform information

    """
    return
