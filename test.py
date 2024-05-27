from osgeo import gdal

# Define the S3 path
s3_path = '/vsis3/fimc-data/benchmark/stac-bench-cat/assets/ble/08020301/100yr/ble_huc_08020301_depth_100yr.tif'


# Generate the signed URL
signed_url = gdal.GetSignedURL(s3_path)

print(f"Signed URL: {signed_url}")

# Use GDAL to access the signed URL
dataset = gdal.Open(signed_url)
if dataset:
    print("Dataset loaded successfully")
else:
    print("Failed to load dataset")
