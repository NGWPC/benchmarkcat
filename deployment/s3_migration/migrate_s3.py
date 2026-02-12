"""
Simple S3 migration with explicit path mappings.

This script:
1. Downloads STAC catalog from source
2. Updates HREFs to point to new data/ structure
3. Generates AWS CLI commands for asset copying
4. Uploads updated catalog to destination stac/

Target structure:
    s3://owp-benchmark/
    ├── stac/                    # STAC metadata
    │   ├── catalog.json
    │   └── collections/
    └── data/                    # Assets organized by collection
        ├── ble-collection/
        ├── gfm-collection/
        ├── iceye-collection/
        └── ...

Usage:
    python migrate_s3.py \\
        --source-bucket fimc-data \\
        --dest-bucket owp-benchmark \\
        --working-dir ~/benchmark-catalog \\
        --aws-profile your-profile \\
        --generate-copy-commands  # Creates shell script to copy assets
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Explicit source → destination mappings
PATH_MAPPINGS = {
    # Collection ID → (source_path, dest_collection_id)
    'ble-collection': {
        'source': 'benchmark/high_resolution_validation_data_ble',
        'dest': 'data/ble-collection'
    },
    'ripple-fim-collection': {
        'source': 'benchmark/ripple_fim_100',
        'dest': 'data/ripple-fim-collection'
    },
    'hwm-collection': {
        'source': 'benchmark/high_water_marks/usgs',
        'dest': 'data/hwm-collection'
    },
    'nws-fim-collection': {
        'source': 'hand_fim/test_cases/nws_test_cases/validation_data_nws',
        'dest': 'data/nws-fim-collection'
    },
    'usgs-fim-collection': {
        'source': 'hand_fim/test_cases/usgs_test_cases/validation_data_usgs',
        'dest': 'data/usgs-fim-collection'
    },
    'gfm-collection': {
        'source': 'benchmark/rs/gfm',
        'dest': 'data/gfm-collection'
    },
    'iceye-collection': {
        'source': 'benchmark/rs/iceye',
        'dest': 'data/iceye-collection'
    },
    'gfm-expanded-collection': {
        'source': 'benchmark/rs/PI4',
        'dest': 'data/gfm-expanded-collection'
    }
}


def update_asset_href(
    old_href: str,
    source_bucket: str,
    dest_bucket: str,
    collection_id: str
) -> Optional[str]:
    """
    Update asset HREF to new structure.

    Args:
        old_href: Original S3 URI or URL
        source_bucket: Source bucket name
        dest_bucket: Destination bucket name
        collection_id: Collection ID for path mapping

    Returns:
        Updated HREF or None if mapping not found
    """
    if collection_id not in PATH_MAPPINGS:
        logger.warning(f"No path mapping for collection: {collection_id}")
        return old_href

    mapping = PATH_MAPPINGS[collection_id]
    source_path = mapping['source']
    dest_path = mapping['dest']

    # Parse S3 URI
    if old_href.startswith('s3://'):
        parsed = urlparse(old_href)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')

        # Only update if it's from the source bucket
        if bucket != source_bucket:
            return old_href

        # Check if key starts with source path
        if not key.startswith(source_path):
            # Try to find source path anywhere in key
            if source_path not in key:
                logger.debug(f"Asset {key} does not match source path {source_path}")
                return old_href

            # Extract relative path after source path
            parts = key.split(source_path)
            if len(parts) > 1:
                relative_path = parts[1].lstrip('/')
            else:
                relative_path = key
        else:
            # Extract relative path
            relative_path = key[len(source_path):].lstrip('/')

        # Build new S3 URI
        new_key = f"{dest_path}/{relative_path}" if relative_path else dest_path
        new_href = f"s3://{dest_bucket}/{new_key}"

        return new_href

    # Handle HTTP URLs
    elif 's3.amazonaws.com' in old_href or 's3-' in old_href:
        if source_bucket in old_href:
            # Extract key from URL
            parts = old_href.split(source_bucket)
            if len(parts) > 1:
                key = parts[-1].lstrip('/').split('?')[0]

                # Try to match source path
                source_path = mapping['source']
                if source_path in key:
                    path_parts = key.split(source_path)
                    relative_path = path_parts[1].lstrip('/')
                    new_key = f"{dest_path}/{relative_path}" if relative_path else dest_path
                    return f"s3://{dest_bucket}/{new_key}"

    return old_href


def download_catalog(
    source_bucket: str,
    source_catalog_prefix: str,
    working_dir: Path,
    aws_profile: Optional[str],
    dry_run: bool
) -> int:
    """Download STAC catalog from S3."""
    logger.info("Phase 1: Downloading catalog from source S3...")

    source_dir = working_dir / 'source_catalog'
    source_dir.mkdir(parents=True, exist_ok=True)

    profile_flag = f"--profile {aws_profile}" if aws_profile else ""

    cmd = (
        f"aws s3 sync s3://{source_bucket}/{source_catalog_prefix}/ "
        f"{source_dir}/ {profile_flag} --exclude '*' --include '*.json'"
    )

    if dry_run:
        logger.info(f"[DRY RUN] Would run: {cmd}")
        return 0

    logger.info(f"Running: {cmd}")
    result = os.system(cmd)

    if result != 0:
        logger.error("Failed to download catalog")
        return result

    json_files = list(source_dir.rglob('*.json'))
    logger.info(f"Downloaded {len(json_files)} JSON files")

    return 0


def update_catalog_hrefs(
    working_dir: Path,
    source_bucket: str,
    dest_bucket: str,
    dry_run: bool
) -> None:
    """Update all HREFs in catalog to new structure."""
    logger.info("Phase 2: Updating HREFs in catalog...")

    source_dir = working_dir / 'source_catalog'
    dest_dir = working_dir / 'dest_catalog'
    dest_dir.mkdir(parents=True, exist_ok=True)

    if not source_dir.exists():
        logger.error(f"Source catalog not found at {source_dir}")
        return

    stats = {
        'collections': 0,
        'items': 0,
        'assets_updated': 0,
        'assets_unchanged': 0
    }

    # Process all JSON files
    json_files = list(source_dir.rglob('*.json'))

    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)

            file_type = data.get('type', '')
            file_modified = False

            # Determine collection ID
            collection_id = None
            if file_type == 'Collection':
                collection_id = data.get('id')
                stats['collections'] += 1
            elif file_type == 'Feature':
                # Extract collection from path or links
                rel_path = json_file.relative_to(source_dir)
                collection_id = rel_path.parts[0] if len(rel_path.parts) > 0 else None

                # Also check collection link
                for link in data.get('links', []):
                    if link.get('rel') == 'collection':
                        collection_href = link.get('href', '')
                        if '/' in collection_href:
                            parts = collection_href.split('/')
                            for part in parts:
                                if 'collection' in part and part in PATH_MAPPINGS:
                                    collection_id = part
                                    break

                stats['items'] += 1

            # Update assets
            if 'assets' in data and collection_id:
                for asset_key, asset in data['assets'].items():
                    if 'href' not in asset or not asset['href']:
                        continue

                    old_href = asset['href']
                    new_href = update_asset_href(
                        old_href,
                        source_bucket,
                        dest_bucket,
                        collection_id
                    )

                    if new_href != old_href:
                        if not dry_run:
                            asset['href'] = new_href
                        else:
                            logger.debug(f"Would update: {old_href} -> {new_href}")

                        file_modified = True
                        stats['assets_updated'] += 1
                    else:
                        stats['assets_unchanged'] += 1

            # Update collection/item links to new stac/ structure
            if 'links' in data:
                for link in data['links']:
                    href = link.get('href', '')

                    # Update relative paths if needed
                    if href.startswith('./') or href.startswith('../'):
                        # Keep relative paths as-is for now
                        continue

                    # Update absolute S3 paths
                    if href.startswith('s3://') and source_bucket in href:
                        # Update catalog/collection links
                        if 'catalog.json' in href or 'collection.json' in href:
                            # These will be in stac/ directory
                            key = href.split(source_bucket)[-1].lstrip('/')
                            # Remove old prefix and add stac/
                            if 'stac-bench-cat' in key:
                                new_key = key.replace('benchmark/stac-bench-cat', 'stac')
                            else:
                                new_key = f"stac/{key}"

                            new_link_href = f"s3://{dest_bucket}/{new_key}"
                            if not dry_run:
                                link['href'] = new_link_href
                            file_modified = True

            # Save updated file
            if file_modified or True:  # Always save to create dest structure
                rel_path = json_file.relative_to(source_dir)
                dest_file = dest_dir / rel_path
                dest_file.parent.mkdir(parents=True, exist_ok=True)

                if not dry_run:
                    with open(dest_file, 'w') as f:
                        json.dump(data, f, indent=2)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse {json_file}: {e}")
        except Exception as e:
            logger.error(f"Error processing {json_file}: {e}")

    logger.info(f"\nProcessed:")
    logger.info(f"  Collections: {stats['collections']}")
    logger.info(f"  Items: {stats['items']}")
    logger.info(f"  Assets updated: {stats['assets_updated']}")
    logger.info(f"  Assets unchanged: {stats['assets_unchanged']}")


def generate_copy_commands(
    source_bucket: str,
    dest_bucket: str,
    working_dir: Path,
    aws_profile: Optional[str]
) -> None:
    """Generate shell script with AWS S3 sync commands for asset copying."""
    logger.info("Phase 3: Generating asset copy commands...")

    script_path = working_dir / 'copy_assets.sh'

    profile_flag = f"--profile {aws_profile}" if aws_profile else ""

    with open(script_path, 'w') as f:
        f.write("#!/bin/bash\n\n")
        f.write("# Generated S3 asset copy commands\n")
        f.write("# This will copy assets from source to destination with new structure\n\n")
        f.write("set -e\n\n")

        f.write("echo 'Starting S3 asset migration...'\n")
        f.write("echo ''\n\n")

        for collection_id, mapping in PATH_MAPPINGS.items():
            source_path = mapping['source']
            dest_path = mapping['dest']

            f.write(f"# {collection_id}\n")
            f.write(f"echo 'Copying {collection_id}...'\n")
            f.write(
                f"aws s3 sync s3://{source_bucket}/{source_path}/ "
                f"s3://{dest_bucket}/{dest_path}/ {profile_flag}\n\n"
            )

        f.write("echo ''\n")
        f.write("echo 'Asset migration complete!'\n")

    # Make executable
    os.chmod(script_path, 0o755)

    logger.info(f"\nAsset copy commands saved to: {script_path}")
    logger.info("Review the script, then run it to copy assets.")
    logger.info(f"\nTo execute: {script_path}")


def upload_catalog(
    dest_bucket: str,
    working_dir: Path,
    aws_profile: Optional[str],
    dry_run: bool
) -> int:
    """Upload updated catalog to destination S3."""
    logger.info("Phase 4: Uploading updated catalog to destination S3...")

    dest_dir = working_dir / 'dest_catalog'

    if not dest_dir.exists():
        logger.error(f"Destination catalog not found at {dest_dir}")
        return 1

    profile_flag = f"--profile {aws_profile}" if aws_profile else ""

    cmd = (
        f"aws s3 sync {dest_dir}/ "
        f"s3://{dest_bucket}/stac/ {profile_flag} "
        f"--exclude '*' --include '*.json'"
    )

    if dry_run:
        logger.info(f"[DRY RUN] Would run: {cmd}")
        return 0

    logger.info(f"Running: {cmd}")
    result = os.system(cmd)

    if result != 0:
        logger.error("Failed to upload catalog")
        return result

    json_files = list(dest_dir.rglob('*.json'))
    logger.info(f"Uploaded {len(json_files)} JSON files to s3://{dest_bucket}/stac/")

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Simple S3 migration with explicit path mappings"
    )
    parser.add_argument(
        '--source-bucket',
        required=True,
        help='Source S3 bucket (e.g., fimc-data)'
    )
    parser.add_argument(
        '--source-catalog-prefix',
        default='benchmark/stac-bench-cat',
        help='Source catalog prefix (default: benchmark/stac-bench-cat)'
    )
    parser.add_argument(
        '--dest-bucket',
        required=True,
        help='Destination S3 bucket (e.g., owp-benchmark)'
    )
    parser.add_argument(
        '--working-dir',
        default='~/benchmark-catalog',
        help='Local working directory (default: ~/benchmark-catalog)'
    )
    parser.add_argument(
        '--aws-profile',
        help='AWS profile to use (optional)'
    )
    parser.add_argument(
        '--generate-copy-commands',
        action='store_true',
        help='Generate shell script for asset copying (Phase 3 only)'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip catalog download (Phase 1)'
    )
    parser.add_argument(
        '--skip-update',
        action='store_true',
        help='Skip HREF update (Phase 2)'
    )
    parser.add_argument(
        '--skip-upload',
        action='store_true',
        help='Skip catalog upload (Phase 4)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview operations without making changes'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    working_dir = Path(args.working_dir).expanduser()
    working_dir.mkdir(parents=True, exist_ok=True)

    logger.info("="*60)
    logger.info("S3 Migration with Restructuring")
    logger.info("="*60)
    logger.info(f"Source: s3://{args.source_bucket}/{args.source_catalog_prefix}/")
    logger.info(f"Destination STAC: s3://{args.dest_bucket}/stac/")
    logger.info(f"Destination Data: s3://{args.dest_bucket}/data/")
    logger.info(f"Working directory: {working_dir}")
    if args.dry_run:
        logger.info("MODE: DRY RUN")
    logger.info("")

    logger.info("Path mappings:")
    for collection_id, mapping in PATH_MAPPINGS.items():
        logger.info(f"  {collection_id}:")
        logger.info(f"    {args.source_bucket}/{mapping['source']}")
        logger.info(f"    -> {args.dest_bucket}/{mapping['dest']}")
    logger.info("")

    # Phase 1: Download catalog
    if not args.skip_download:
        result = download_catalog(
            args.source_bucket,
            args.source_catalog_prefix,
            working_dir,
            args.aws_profile,
            args.dry_run
        )
        if result != 0:
            sys.exit(result)
    else:
        logger.info("Skipping catalog download (--skip-download)")

    # Phase 2: Update HREFs
    if not args.skip_update:
        update_catalog_hrefs(
            working_dir,
            args.source_bucket,
            args.dest_bucket,
            args.dry_run
        )
    else:
        logger.info("Skipping HREF update (--skip-update)")

    # Phase 3: Generate copy commands or skip
    if args.generate_copy_commands or not (args.skip_download and args.skip_update):
        generate_copy_commands(
            args.source_bucket,
            args.dest_bucket,
            working_dir,
            args.aws_profile
        )

    # Phase 4: Upload catalog
    if not args.skip_upload:
        result = upload_catalog(
            args.dest_bucket,
            working_dir,
            args.aws_profile,
            args.dry_run
        )
        if result != 0:
            sys.exit(result)
    else:
        logger.info("Skipping catalog upload (--skip-upload)")

    logger.info("\n" + "="*60)
    logger.info("Migration Script Complete!")
    logger.info("="*60)
    logger.info("\nNext steps:")
    logger.info(f"1. Review updated catalog in: {working_dir}/dest_catalog/")
    logger.info(f"2. Run asset copy script: {working_dir}/copy_assets.sh")
    logger.info(f"3. Verify uploads:")
    logger.info(f"   aws s3 ls s3://{args.dest_bucket}/stac/ --recursive | wc -l")
    logger.info(f"   aws s3 ls s3://{args.dest_bucket}/data/ --recursive | wc -l")


if __name__ == "__main__":
    main()
