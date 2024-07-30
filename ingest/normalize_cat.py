import pystac
import sys
import os

def normalize_catalog(catalog_path: str, s3_root_href: str):
    """
    Normalize all the relative catalog links relative to an S3 catalog root.

    Parameters:
    catalog_path (str): The local path to the STAC catalog file.
    s3_root_href (str): The root HREF of the S3 bucket to normalize links against.
    """
    # Load the catalog from the local file
    catalog = pystac.Catalog.from_file(catalog_path)

    # Normalize HREFs to the given root HREF
    catalog.normalize_hrefs(s3_root_href)

    # Save the catalog back to the S3 root
    catalog.save(dest_href=os.path.dirname(catalog_path),catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED)

    print(f"Catalog normalized and saved with root HREF: {s3_root_href}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python -m mystac.normalize_stac <catalog_path> <s3_root_href>")
        sys.exit(1)

    catalog_path = sys.argv[1]
    s3_root_href = sys.argv[2]

    if not os.path.exists(catalog_path):
        print(f"Error: The specified catalog path does not exist: {catalog_path}")
        sys.exit(1)

    normalize_catalog(catalog_path, s3_root_href)
