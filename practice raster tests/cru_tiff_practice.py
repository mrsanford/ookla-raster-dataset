# bands represent yearly mean precipitation in millimeters
# open this raster with Python and see its metadata
# resource_link = https://geobgu.xyz/py/10-rasterio1.html
import rasterio
import numpy as np
from affine import Affine
from rasterio.crs import CRS

# while I know this is a valid solve, does this mean that analysis of the metadata is necessary before 
# writing the raster file (aka is automation a thing?)
cru_metadata = {
    'driver': 'GTiff',
    'height': 360,
    'width': 720,
    'count': 1,
    'dtype': 'float64',
    'crs': CRS.from_epsg(4326),
    'transform': Affine(0.5, 0.0, -180.0, 0.0, -0.5, 90.0)
    }
#  CRS.from_epsg(3857)
# 65346 x 65346

# I opened the raster in read. Then read the corresponding 1st src band
with rasterio.open('/Users/michellesanford/GitHub/cru.pre.2022.tif', "r") as src:
    data = src.read(1)
    print(src.meta)

# I created the new output raster layer and started writing in the file
# I am writing from the data that I'm reading from src, and only the one band
with rasterio.open('cru_output.tif','w',**cru_metadata) as dst:
    dst.write(data,1)
print('Raster created')

# Then opening the new .tif and finding the metadata
# (and reading it back to me to confirm)
with rasterio.open('cru_output.tif') as src:
    print(src.meta)
    new_data = src.read(1)
    print(data.shape)