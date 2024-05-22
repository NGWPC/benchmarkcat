import numpy as np
import rasterio

def count_nonzero_pixels(raster_path):
    # Open the raster file
    with rasterio.open(raster_path) as dataset:
        # Read the first band
        band1 = dataset.read(1)
        
        # Count the non-zero pixels
        nonzero_count = np.count_nonzero(band1)
        
    return nonzero_count
