import tempfile
import io
import json
import pystac
import os
from botocore.exceptions import NoCredentialsError, ClientError, ParamValidationError
import requests

class S3Utils:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def make_and_upload_thumbnail(self, local_asset_path, local_thumbnail_path, bucket_name, s3_path):
        try:
            # Download the file from S3
            self.s3_client.download_file(bucket_name, s3_path, local_asset_path)
            print(f"Downloaded extent raster to {local_asset_path}")
            # Create thumbnail
            RasterUtils.create_preview(local_asset_path, local_thumbnail_path)

            # Upload thumbnail to S3
            s3_dir = os.path.dirname(s3_path)  
            filename = os.path.basename(local_thumbnail_path) 
            thumbnail_s3_path = os.path.join(s3_dir, filename) 
            self.s3_client.upload_file(local_thumbnail_path, bucket_name, thumbnail_s3_path)
            print(f"Uploaded thumbnail to s3://{bucket_name}/{thumbnail_s3_path}")

            return thumbnail_s3_path
            
        except NoCredentialsError:
            print("Credentials not available")
            return None
        except ClientError as e:
            print(f"Failed to download or upload files: {e}")
            return None

    def list_s3_objects(self, bucket, prefix, filter_func=None, process_func=None, delimiter=None):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        operation_parameters = {
            'Bucket': bucket,
            'Prefix': prefix
        }
        if delimiter:
            operation_parameters['Delimiter'] = delimiter

        pages = paginator.paginate(**operation_parameters)
        
        results = []
        for page in pages:
            if delimiter and 'CommonPrefixes' in page:
                for common_prefix in page['CommonPrefixes']:
                    prefix = common_prefix['Prefix']
                    if filter_func is None or filter_func({'Key': prefix}):
                        if process_func:
                            results.append(process_func(bucket, {'Key': prefix}))
                        else:
                            results.append(prefix)
            else:
                for obj in page.get('Contents', []):
                    if filter_func is None or filter_func(obj):
                        if process_func:
                            results.append(process_func(bucket, obj))
                        else:
                            results.append(obj['Key'])
        return results

    def list_files_with_extensions(self, bucket, prefix, extensions):
        def filter_files_with_extensions(obj):
            return any(obj['Key'].endswith(ext) for ext in extensions)
        
        def process_file(bucket, obj):
            return obj['Key']
        
        return self.list_s3_objects(bucket, prefix, filter_files_with_extensions, process_file)

    def list_subdirectories(self, bucket_name, prefix):
        return self.list_s3_objects(bucket_name, prefix, delimiter='/')

    def list_resources_with_string(self, bucket, prefix, keywords, delimiter=None):
        def filter_func(obj):
            return any(keyword in obj['Key'] for keyword in keywords)
        
        def process_func(bucket, obj):
            return obj['Key']
        
        return self.list_s3_objects(bucket, prefix, filter_func, process_func, delimiter=delimiter)

    def download_catalog_and_collections(self, catalog_key, bucket_name, tmp_dir):
        catalog_response = self.s3_client.get_object(Bucket=bucket_name, Key=catalog_key)
        catalog_content = catalog_response['Body'].read().decode('utf-8')
        catalog_dict = json.load(io.StringIO(catalog_content))
        
        catalog_local_path = os.path.join(tmp_dir, os.path.basename(catalog_key))
        with open(catalog_local_path, 'w') as f:
            json.dump(catalog_dict, f, indent=4)

        catalog = pystac.Catalog.from_dict(catalog_dict)
        
        for link in catalog.get_child_links():
            child_relative_path = link.target
            catalog_dir = os.path.dirname(catalog_key)
            child_s3_key = os.path.normpath(os.path.join(catalog_dir, child_relative_path))
            child_local_path = os.path.join(tmp_dir, child_relative_path)
            
            os.makedirs(os.path.dirname(child_local_path), exist_ok=True)
            
            child_response = self.s3_client.get_object(Bucket=bucket_name, Key=child_s3_key)
            child_content = child_response['Body'].read().decode('utf-8')
            child_dict = json.load(io.StringIO(child_content))
            
            with open(child_local_path, 'w') as f:
                json.dump(child_dict, f, indent=4)
        
        return catalog, catalog_local_path

    def upload_directory_to_s3(self, directory_path, bucket_name, destination_path):
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                s3_key = os.path.join(destination_path, os.path.relpath(file_path, directory_path))
                try:
                    self.s3_client.upload_file(file_path, bucket_name, s3_key)
                    print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
                except (NoCredentialsError, ClientError) as e:
                    print(f"Failed to upload {file_path} to s3://{bucket_name}/{s3_key}: {e}")

    def update_collection(self, collection, catalog_id, catalog_path, bucket_name):
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog_key = f'{catalog_path}catalog.json'
            catalog, catalog_local_path = self.download_catalog_and_collections(catalog_key, bucket_name, temp_dir)

            catalog.set_root(catalog)
            catalog.set_self_href(catalog_local_path)

            try:
                catalog.remove_child(catalog_id)
            except KeyError:
                pass

            catalog.add_child(collection)

            catalog.normalize_and_save(root_href=temp_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED, skip_unresolved=True)

            self.upload_directory_to_s3(temp_dir, bucket_name, catalog_path)

    def generate_href(self, bucket_name, path, link_type, expiration=7*24*60*60):
        try:
            if link_type == 'url':
                # Generate presigned URL
                signed_url = self.s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': path},
                    ExpiresIn=expiration
                )
            
                # Validate URL
                try:
                    response = requests.head(signed_url, timeout=5)
                    is_valid = response.status_code == 200
                except requests.RequestException as e:
                    is_valid = False
            
                return signed_url, is_valid 
            
            elif link_type == 'uri':
                # Generate S3 URI
                s3_uri = f"s3://{bucket_name}/{path}"
            
                # Validate object exists
                try:
                    self.s3_client.head_object(Bucket=bucket_name, Key=path)
                    is_valid = True
                except (ClientError, ParamValidationError) as e:
                    is_valid = False
            
                return s3_uri, is_valid 
            
            else:
                raise ValueError("link_type must be either 'url' or 'uri'")

        except NoCredentialsError:
            raise ValueError("Credentials not available")

