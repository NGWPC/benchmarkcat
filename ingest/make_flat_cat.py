import json
import pystac
import os

def resolve_path(base_path, relative_path):
    if os.path.isabs(relative_path):
        return relative_path
    return os.path.join(os.path.dirname(base_path), relative_path)

def flatten_catalog(catalog_or_collection, base_path):
    flattened = catalog_or_collection.to_dict()
    
    # Process all child links
    new_links = []
    for link in flattened['links']:
        if link['rel'] == 'child':
            child_path = resolve_path(base_path, link['href'])
            try:
                child = pystac.read_file(child_path)
                # Recursively flatten the child
                flattened_child = flatten_catalog(child, child_path)
                # Replace the link with the flattened child content
                new_links.append(flattened_child)
            except FileNotFoundError:
                print(f"Warning: Could not find file {child_path}. Skipping this child.")
                new_links.append(link)
        else:
            new_links.append(link)
    
    flattened['links'] = new_links

    # If it's a collection, flatten all its items
    if 'type' in flattened and flattened['type'] == 'Collection':
        flattened_items = []
        for item in pystac.read_file(base_path).get_all_items():
            flattened_items.append(item.to_dict())
        flattened['items'] = flattened_items

    return flattened

def main():
    # Load the STAC catalog
    catalog_path = input("Enter the path to your STAC catalog JSON file: ")
    catalog_path = os.path.abspath(catalog_path)
    try:
        catalog = pystac.read_file(catalog_path)
    except FileNotFoundError:
        print(f"Error: Could not find the catalog file at {catalog_path}")
        return

    # Flatten the catalog
    flattened_catalog = flatten_catalog(catalog, catalog_path)

    # Save the flattened catalog to a new JSON file
    output_path = input("Enter the path for the output flattened catalog JSON file: ")
    output_path = os.path.abspath(output_path)
    with open(output_path, 'w') as f:
        json.dump(flattened_catalog, f, indent=2)

    print(f"Flattened catalog saved to {output_path}")

if __name__ == "__main__":
    main()
