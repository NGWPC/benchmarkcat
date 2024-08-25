import json
import msgpack
import os
import sys
from tqdm import tqdm

def json_to_msgpack(json_file, msgpack_file):
    total_size = os.path.getsize(json_file)
    chunk_size = 1024 * 1024  # 1 MB chunks

    try:
        with open(json_file, 'r') as jf, open(msgpack_file, 'wb') as mf:
            pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc="Converting")
            packer = msgpack.Packer()

            # Read the entire file content
            data = json.load(jf)

            # Pack the entire data at once
            packed_data = packer.pack(data)

            # Write packed data in chunks
            for i in range(0, len(packed_data), chunk_size):
                chunk = packed_data[i:i+chunk_size]
                mf.write(chunk)
                pbar.update(len(chunk))

            pbar.close()

        print(f"Conversion completed. MessagePack file saved as: {msgpack_file}")

        # Print file size comparison
        json_size = os.path.getsize(json_file)
        msgpack_size = os.path.getsize(msgpack_file)
        print(f"Original JSON size: {json_size:,} bytes")
        print(f"MessagePack size: {msgpack_size:,} bytes")
        print(f"Size reduction: {(1 - msgpack_size/json_size)*100:.2f}%")

    except IOError as e:
        print(f"IO Error: {e}")
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_json_file> <output_msgpack_file>")
        sys.exit(1)

    json_file = sys.argv[1]
    msgpack_file = sys.argv[2]

    json_to_msgpack(json_file, msgpack_file)

