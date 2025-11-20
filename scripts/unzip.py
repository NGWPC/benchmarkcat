import os
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

def main():
    '''
    Pass two arguments, a source directory containing .zip files and a destination directory where to unzip
    those files. Keep the filename of the original zip as the new directory name. This by default creates
    a nested directory structure, so flatten the redundant child directory.

    After running this script, you can use this bash oneline to see the consistence of the extracted files:
    find . -type d | while read -r dir; do
    count=$(find "$dir" -maxdepth 1 -type f | wc -l)
    echo "$dir: $count files"
    done

    Finally, push new destination directory to a different S3 bucket to reference from STAC. s
    '''
    parser = argparse.ArgumentParser(description="Unzip .zip files from source to destination")
    parser.add_argument('source_dir', type=str, help="The source directory containing .zip files")
    parser.add_argument('dest_dir', type=str, help="The destination directory to extract to")
    
    args = parser.parse_args()

    unzip_files(args.source_dir, args.dest_dir)

    copy_non_zip_files(args.source_dir, args.dest_dir)

    flatten_directory_structure(args.dest_dir)

if __name__ == '__main__':
    main()
