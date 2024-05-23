import rioxarray

def count_nonzero_pixels(raster_path):
    """Function to count nonzero pixels in a raster."""
    raster = rioxarray.open_rasterio(raster_path, masked=True, chunks=True)
    # Select the first band
    band1 = raster.sel(band=1)
    # Count the non-zero pixels
    nonzero_count = band1.count().compute().item()
    return nonzero_count

