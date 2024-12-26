from pyproj import CRS
import pygeohydro as pgh
import rioxarray
import rasterio
import numpy as np
from PIL import Image

class RasterUtils:
    @staticmethod
    def create_preview(raster_path, preview_path, size=(256, 256), chunk_size=1024):
        """Create preview using chunked processing with rioxarray.
    
        Args:
            raster_path: Path to input raster file
            preview_path: Path to save preview image
            size: Tuple of (width, height) for final preview size
            chunk_size: Size of chunks for processing
        """
        # Open the raster with chunking
        raster = rioxarray.open_rasterio(
            raster_path,
            masked=True,
            chunks={'x': chunk_size, 'y': chunk_size}
        )
        band1 = raster.sel(band=1)
    
        # Get input dimensions
        in_height, in_width = band1.shape
    
        # Calculate initial target size maintaining aspect ratio
        ratio = in_width / in_height
        max_width, max_height = size
    
        # Calculate intermediate size that ensures factors > 0
        # Start with the smaller dimension and scale up
        if in_height < in_width:  # wide image
            # Make sure intermediate height is smaller than input height
            inter_height = min(chunk_size, in_height - 1)
            inter_width = int(inter_height * ratio)
        else:  # tall image
            # Make sure intermediate width is smaller than input width
            inter_width = min(chunk_size, in_width - 1)
            inter_height = int(inter_width / ratio)
    
        # Calculate reduction factors (guaranteed to be >= 1)
        y_factor = max(1, in_height // inter_height)
        x_factor = max(1, in_width // inter_width)
    
        # Use coarsen to reduce the size
        coarsened = band1.coarsen(
            y=y_factor,
            x=x_factor,
            boundary='trim'
        ).any()
    
        # Compute the result
        result = coarsened.compute()
    
        # Convert to RGBA
        img_data_rgba = np.zeros((*result.shape, 4), dtype=np.uint8)
        img_data_rgba[~result] = [255, 255, 255, 255]  # White for 0/False
        img_data_rgba[result] = [0, 0, 0, 255]         # Black for non-zero/True
    
        # Create PIL image and resize to final size
        pil_image = Image.fromarray(img_data_rgba, 'RGBA')
    
        # Calculate final dimensions maintaining aspect ratio
        scale = min(max_width / result.shape[1], max_height / result.shape[0])
        new_width = int(result.shape[1] * scale)
        new_height = int(result.shape[0] * scale)
    
        preview = pil_image.resize((new_width, new_height), resample=Image.Resampling.LANCZOS)
        preview.save(preview_path, format="PNG")

    @staticmethod
    def count_pixels(raster_path, values=None):
        raster = rioxarray.open_rasterio(raster_path, masked=True, chunks=True)
        band1 = raster.sel(band=1)
        
        if values is None:
            pixel_count = (band1 != 0).sum().compute().item()
        else:
            mask = False
            for value in values:
                mask |= (band1 == value)
            pixel_count = mask.sum().compute().item()
        
        return pixel_count

    @staticmethod
    def get_wkt2_string(raster_path):
        with rasterio.open(raster_path) as src:
            crs_info = src.crs.to_wkt()
            if crs_info:
                wkt = CRS.from_wkt(crs_info)
                wkt2_string = wkt.to_wkt(version='WKT2_2018_SIMPLIFIED')
                return wkt2_string
            else:
                raise ValueError(f"EPSG code not found for raster: {raster_path}")

    @staticmethod
    def get_huc8_geometry(huc8):
        wbd = pgh.WBD("huc8")
        huc8_geom = wbd.byids("huc8", [huc8])
        return huc8_geom.geometry.iloc[0]
