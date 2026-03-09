#!/usr/bin/env python3
"""
Rewrite S3 asset URLs in pgstac database to use asset-proxy service.

This script updates STAC item assets that use S3 URIs (s3://bucket/path) or
direct S3 HTTPS URLs to use the asset-proxy service, enabling browser
access to private/requester-pays S3 buckets.

Usage:
    # Dry run to preview changes (local access only)
    python3 rewrite_asset_urls.py --proxy-url http://localhost:8083 --dry-run

    # Apply changes for local access
    python3 rewrite_asset_urls.py --proxy-url http://localhost:8083

    # For VPC access, use host IP
    export HOST_IP=$(hostname -I | awk '{print $1}')
    python3 rewrite_asset_urls.py --proxy-url http://${HOST_IP}:8083

    # Use external domain name
    python3 rewrite_asset_urls.py --proxy-url http://your-domain.com:8083
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor


def parse_s3_url(url: str) -> tuple[str | None, str | None]:
    """
    Parse S3 URL to extract bucket and key.

    Args:
        url: S3 URL in format s3://bucket/key or https://bucket.s3.amazonaws.com/key
             or existing proxy URL like http://host:8083/s3/bucket/key

    Returns:
        Tuple of (bucket, key) or (None, None) if not an S3 URL
    """
    # Handle existing proxy URLs: http://hostname:8083/s3/bucket/key
    if '/s3/' in url:
        parsed = urlparse(url)
        # Extract path after /s3/
        path_parts = parsed.path.split('/s3/', 1)
        if len(path_parts) == 2:
            s3_path = path_parts[1]
            parts = s3_path.split('/', 1)
            if len(parts) == 2:
                bucket, key = parts
                return bucket, key

    # Handle s3:// URIs
    if url.startswith('s3://'):
        parsed = urlparse(url)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        return bucket, key

    # Handle https://bucket.s3.amazonaws.com/key or https://s3.amazonaws.com/bucket/key
    if 's3.amazonaws.com' in url or 's3-' in url:
        parsed = urlparse(url)

        # Format: https://bucket.s3.amazonaws.com/key or https://bucket.s3.region.amazonaws.com/key
        if parsed.netloc.endswith('.s3.amazonaws.com') or '.s3-' in parsed.netloc or '.s3.' in parsed.netloc:
            bucket = parsed.netloc.split('.')[0]
            key = parsed.path.lstrip('/')
            return bucket, key

        # Format: https://s3.amazonaws.com/bucket/key or https://s3.region.amazonaws.com/bucket/key
        if parsed.netloc.startswith('s3.') or parsed.netloc.startswith('s3-'):
            parts = parsed.path.lstrip('/').split('/', 1)
            if len(parts) == 2:
                bucket, key = parts
                return bucket, key

    return None, None


def rewrite_asset_href(href: str, proxy_base_url: str) -> str | None:
    """
    Rewrite S3 asset URL to use proxy service.

    Args:
        href: Original asset href
        proxy_base_url: Base URL of asset-proxy service (e.g., http://localhost:8083)

    Returns:
        Rewritten URL or None if not an S3 URL
    """
    bucket, key = parse_s3_url(href)

    if bucket and key:
        # Rewrite to proxy format: http://proxy-url/s3/bucket/key
        proxy_url = f"{proxy_base_url.rstrip('/')}/s3/{bucket}/{key}"
        return proxy_url

    return None


def get_items_with_assets(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    """
    Get all items from pgstac database.

    Args:
        conn: Database connection

    Returns:
        List of items with id, collection, and content (JSONB)
    """
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("""
        SELECT id, collection, content
        FROM pgstac.items
        ORDER BY collection, id;
    """)

    items = cursor.fetchall()
    cursor.close()

    return items


def update_item_assets(
    conn: psycopg2.extensions.connection,
    item_id: str,
    collection: str,
    updated_content: dict[str, Any],
    dry_run: bool = False
) -> bool:
    """
    Update item content in database.

    Args:
        conn: Database connection
        item_id: Item ID
        collection: Collection name
        updated_content: Updated item JSONB content
        dry_run: If True, don't actually update

    Returns:
        True if successful
    """
    if dry_run:
        return True

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE pgstac.items
            SET content = %s::jsonb
            WHERE id = %s AND collection = %s;
            """,
            (json.dumps(updated_content), item_id, collection)
        )

        conn.commit()
        cursor.close()
        return True

    except Exception as e:
        conn.rollback()
        print(f"    ERROR updating item {item_id}: {e}")
        return False


def rewrite_all_assets(
    conn: psycopg2.extensions.connection,
    proxy_base_url: str,
    dry_run: bool = False
) -> dict[str, int]:
    """
    Rewrite all S3 asset URLs in database to use proxy service.

    Args:
        conn: Database connection
        proxy_base_url: Base URL of asset-proxy service
        dry_run: If True, only show what would be changed

    Returns:
        Statistics dictionary
    """
    stats = {
        'total_items': 0,
        'items_with_assets': 0,
        'items_updated': 0,
        'assets_rewritten': 0,
        'items_failed': 0
    }

    print("Fetching items from database...")
    items = get_items_with_assets(conn)
    stats['total_items'] = len(items)

    print(f"Found {len(items)} items")
    print()

    for item in items:
        item_id = item['id']
        collection = item['collection']
        content = item['content']

        # Skip if no assets
        if 'assets' not in content or not content['assets']:
            continue

        stats['items_with_assets'] += 1

        # Check and rewrite asset hrefs
        modified = False
        asset_changes = []

        for asset_key, asset_data in content['assets'].items():
            if 'href' not in asset_data:
                continue

            original_href = asset_data['href']

            # Try to rewrite
            new_href = rewrite_asset_href(original_href, proxy_base_url)

            if new_href:
                asset_data['href'] = new_href
                modified = True
                stats['assets_rewritten'] += 1
                asset_changes.append((asset_key, original_href, new_href))

        # Update item if modified
        if modified:
            print(f"Item: {collection}/{item_id}")
            for asset_key, old_href, new_href in asset_changes:
                print(f"  {asset_key}:")
                print(f"    OLD: {old_href}")
                print(f"    NEW: {new_href}")

            if update_item_assets(conn, item_id, collection, content, dry_run):
                stats['items_updated'] += 1
                if dry_run:
                    print("    [DRY RUN] Would update")
                else:
                    print("    ✓ Updated")
            else:
                stats['items_failed'] += 1

            print()

    return stats


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Rewrite S3 asset URLs to use asset-proxy service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run with localhost proxy (local access only)
  python3 rewrite_asset_urls.py --proxy-url http://localhost:8083 --dry-run

  # Apply changes with localhost proxy (local access only)
  python3 rewrite_asset_urls.py --proxy-url http://localhost:8083

  # For VPC access, use host IP
  export HOST_IP=$(hostname -I | awk '{print $1}')
  python3 rewrite_asset_urls.py --proxy-url http://${HOST_IP}:8083

  # Use external domain
  python3 rewrite_asset_urls.py --proxy-url http://ec2-instance.compute.amazonaws.com:8083
        """
    )

    parser.add_argument(
        '--proxy-url',
        required=True,
        help='Base URL of asset-proxy service (e.g., http://localhost:8083)'
    )

    parser.add_argument(
        '--db-host',
        default=os.environ.get('PGHOST', 'localhost'),
        help='Database host (default: localhost)'
    )

    parser.add_argument(
        '--db-port',
        type=int,
        default=5432,
        help='Database port (default: 5432)'
    )

    parser.add_argument(
        '--db-user',
        default='pgstac',
        help='Database user (default: pgstac)'
    )

    parser.add_argument(
        '--db-password',
        default=None,
        help='Database password (or set PGPASSWORD env var)'
    )

    parser.add_argument(
        '--db-name',
        default='stacdb',
        help='Database name (default: stacdb)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without actually updating'
    )

    args = parser.parse_args()

    # Get password
    password = args.db_password or os.environ.get('PGPASSWORD')

    if password is None:
        password_file = Path('/opt/benchmarkcat/.db_password')
        if password_file.exists():
            password = password_file.read_text().strip()

    if password is None:
        print("ERROR: Database password required")
        print("  Set via --db-password, PGPASSWORD env var, or /opt/benchmarkcat/.db_password")
        return 1

    print("=" * 80)
    print("STAC Asset URL Rewriter")
    print("=" * 80)
    print()
    print(f"Proxy Base URL: {args.proxy_url}")
    print(f"Database: {args.db_host}:{args.db_port}/{args.db_name}")

    if args.dry_run:
        print()
        print("DRY RUN MODE: No changes will be made")

    print()

    # Connect to database
    try:
        conn = psycopg2.connect(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=password,
            database=args.db_name
        )
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        return 1

    # Rewrite assets
    stats = rewrite_all_assets(conn, args.proxy_url, args.dry_run)

    conn.close()

    # Print summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total items:         {stats['total_items']}")
    print(f"Items with assets:   {stats['items_with_assets']}")
    print(f"Items updated:       {stats['items_updated']}")
    print(f"Assets rewritten:    {stats['assets_rewritten']}")
    print(f"Items failed:        {stats['items_failed']}")

    if args.dry_run:
        print()
        print("[DRY RUN] No changes were made to the database")

    print()

    return 0


if __name__ == '__main__':
    sys.exit(main())
