import os
import re
import zipfile
import argparse
import shutil

def unzip_files(source_dir, dest_dir):
    # Ensure the destination directory exists
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # Get total amount of zip files
    total_zip_files = sum(
        1 for f in os.listdir(source_dir)
        if os.path.isfile(os.path.join(source_dir, f)) and f.lower().endswith('.zip')
    )
    count = 0 
    # Loop through files in the source directory
    for filename in os.listdir(source_dir):
        count += 1
        # Check if the file is a .zip file
        if filename.endswith('.zip'):
            zip_path = os.path.join(source_dir, filename)
            # Remove the .zip extension to create a new folder name
            folder_name = os.path.splitext(filename)[0]
            folder_path = os.path.join(dest_dir, folder_name)

            # Ensure the folder for extraction exists (no nesting issue)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            # Extract the zip file into the folder
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                print(f'Extracting {filename} to {folder_path}...')
                zip_ref.extractall(folder_path)
            print(f'{count}/{total_zip_files} → {filename} extracted successfully!')

def copy_non_zip_files(source_dir, dest_dir):
    """
    Move all files that do NOT have a .zip extension from source_dir to dest_dir.
    Creates dest_dir if it does not exist.
    """
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    for filename in os.listdir(source_dir):
        src_path = os.path.join(source_dir, filename)
        dest_path = os.path.join(dest_dir, filename)

        # Skip directories
        if os.path.isdir(src_path):
            continue

        # Move only non-.zip files
        if not filename.lower().endswith('.zip'):
            # Handle name collisions
            if os.path.exists(dest_path):
                base, ext = os.path.splitext(filename)
                i = 1
                while os.path.exists(os.path.join(dest_dir, f"{base}_{i}{ext}")):
                    i += 1
                dest_path = os.path.join(dest_dir, f"{base}_{i}{ext}")

            shutil.copy(src_path, dest_path)
            print(f"Copied: {filename} → {dest_dir}")

def flatten_directory_structure(base_dir):
    """
    Move all files from nested subdirectories (grandchildren and deeper)
    into their immediate parent directories, and remove only empty
    grandchild directories (not direct children of base_dir).
    """
    for root, dirs, files in os.walk(base_dir, topdown=False):
        # Skip removing or moving from the base directory itself
        if root == base_dir:
            continue

        for file in files:
            src = os.path.join(root, file)
            parent_dir = os.path.dirname(root)

            # If the parent is still within base_dir, move up one level
            # but don't flatten direct children of base_dir
            if parent_dir == base_dir:
                # File is already one level deep — do nothing
                continue

            # Move file one level up
            dst = os.path.join(parent_dir, file)

            # Handle duplicate filenames
            if os.path.exists(dst):
                base, ext = os.path.splitext(file)
                i = 1
                while os.path.exists(os.path.join(parent_dir, f"{base}_{i}{ext}")):
                    i += 1
                dst = os.path.join(parent_dir, f"{base}_{i}{ext}")

            shutil.move(src, dst)
            print(f"Moved {file} to {parent_dir}")

        # Remove the directory if it's now empty AND it's not a direct child of base_dir
        if not os.listdir(root) and os.path.dirname(root) != base_dir:
            print(f"Removing empty grandchild directory: {root}")
            os.rmdir(root)

def extract_nested_zips(base_dir: str) -> None:
    """Extract any .zip files found inside subdirectories, then remove them.

    Runs repeatedly until no nested zips remain (handles zips within zips).
    """
    while True:
        nested_zips = []
        for root, _dirs, files in os.walk(base_dir):
            if root == base_dir:
                continue
            for f in files:
                if f.lower().endswith('.zip'):
                    nested_zips.append(os.path.join(root, f))

        if not nested_zips:
            break

        for zpath in nested_zips:
            parent = os.path.dirname(zpath)
            print(f"Extracting nested zip: {os.path.basename(zpath)}")
            with zipfile.ZipFile(zpath, 'r') as zf:
                zf.extractall(parent)
            os.remove(zpath)

        flatten_directory_structure(base_dir)

    print(f"No more nested zips in {base_dir}")


def consolidate_dangling_files(base_dir: str, source_dir: str | None = None) -> None:
    """Move loose files into per-revision directories (FSD ID + R#).

    Groups files by their FSD event ID and revision number, e.g.
    ICEYE_FSD-2119_flood_depth_usa_ohio_valley_north_in_R6.tif → directory
    matching FSD-2119 + R6. If no existing directory matches, one is created
    using the original zip filename from source_dir (falling back to the
    dangling file's stem if source_dir is not provided).
    """
    revision_pattern = re.compile(r"(ICEYE_FSD[-_]\d+).*_(R\d+)")

    # Build map of (fsd_id, revision) → zip stem from source directory
    # Normalize FSD_XXXX to FSD-XXXX so underscore variants match hyphen keys
    def normalize_fsd(fsd_id: str) -> str:
        return re.sub(r"FSD_(\d+)", r"FSD-\1", fsd_id)

    zip_names: dict[tuple[str, str], str] = {}
    if source_dir:
        for entry in os.listdir(source_dir):
            if entry.lower().endswith('.zip'):
                match = revision_pattern.search(entry)
                if match:
                    key = (normalize_fsd(match.group(1)), match.group(2))
                    # Prefer "flood_insights" zips as they are the primary deliverable
                    if key not in zip_names or "flood_insights" in entry:
                        # Normalize the zip stem name to use hyphen as well
                        zip_names[key] = re.sub(r"FSD_(\d+)", r"FSD-\1", os.path.splitext(entry)[0])

    # Build map of (fsd_id, revision) → existing directory
    key_to_dir: dict[tuple[str, str], str] = {}
    for entry in os.listdir(base_dir):
        if os.path.isdir(os.path.join(base_dir, entry)):
            match = revision_pattern.search(entry)
            if match:
                key = (normalize_fsd(match.group(1)), match.group(2))
                if key not in key_to_dir:
                    key_to_dir[key] = os.path.join(base_dir, entry)

    moved = 0
    for filename in list(os.listdir(base_dir)):
        filepath = os.path.join(base_dir, filename)
        if os.path.isdir(filepath):
            continue

        match = revision_pattern.search(filename)
        if not match:
            print(f"Skipping {filename} — no FSD ID + revision found")
            continue

        key = (normalize_fsd(match.group(1)), match.group(2))
        target_dir = key_to_dir.get(key)

        if target_dir is None:
            dir_name = zip_names.get(key, os.path.splitext(filename)[0])
            target_dir = os.path.join(base_dir, dir_name)
            os.makedirs(target_dir, exist_ok=True)
            key_to_dir[key] = target_dir
            print(f"Created directory for {key[0]} {key[1]}: {dir_name}")

        dst = os.path.join(target_dir, filename)
        if os.path.exists(dst):
            base, ext = os.path.splitext(filename)
            i = 1
            while os.path.exists(os.path.join(target_dir, f"{base}_{i}{ext}")):
                i += 1
            dst = os.path.join(target_dir, f"{base}_{i}{ext}")

        shutil.move(filepath, dst)
        moved += 1

    print(f"Consolidated {moved} dangling files into revision directories")


def main():
    '''
    Unzip, flatten, and consolidate ICEYE data deliveries.

    Usage:
        python unzip.py /path/to/zips /path/to/output

    Steps:
        1. Extracts each .zip into a directory named after the zip
        2. Copies loose (non-zip) files from source into dest
        3. Flattens nested subdirectories
        4. Consolidates dangling files into per-revision directories

    Flatten nested directories separately (e.g. after a partial re-extract):
        python -c "from scripts.unzip import flatten_directory_structure; flatten_directory_structure('/path/to/output')"

    Consolidate dangling files separately:
        python -c "from scripts.unzip import consolidate_dangling_files; consolidate_dangling_files('/path/to/output')"

    Verify output consistency:
        find /path/to/output -type d | while read -r dir; do
            count=$(find "$dir" -maxdepth 1 -type f | wc -l)
            echo "$dir: $count files"
        done
    '''
    parser = argparse.ArgumentParser(description="Unzip .zip files from source to destination")
    parser.add_argument('source_dir', type=str, help="The source directory containing .zip files")
    parser.add_argument('dest_dir', type=str, help="The destination directory to extract to")
    
    args = parser.parse_args()

    unzip_files(args.source_dir, args.dest_dir)

    copy_non_zip_files(args.source_dir, args.dest_dir)

    flatten_directory_structure(args.dest_dir)

    extract_nested_zips(args.dest_dir)

    consolidate_dangling_files(args.dest_dir, args.source_dir)

if __name__ == '__main__':
    main()
