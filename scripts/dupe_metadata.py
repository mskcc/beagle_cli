import os
import argparse
from dotenv import load_dotenv
from bin.access_beagle_endpoint import AccessBeagleEndpoint

# Load environment variables from .env file
load_dotenv()

def dupe_files(source_slug, dest_slug, request_id, **kwargs):
    endpoint = AccessBeagleEndpoint()
    file_ids = endpoint.get_file_ids(request_id, source_slug)

    # For every file_id and its metadata, copy it over into dest_slug
    for file_id in file_ids:
        file_meta = endpoint.get_file_metadata(file_id)
        file_type = file_meta["file_type"]
        file_path = file_meta["path"]
        file_metadata = file_meta["metadata"]

        file_metadata.update(kwargs)
        file_group_id = endpoint.get_file_group_id_by_slug(dest_slug)
        print(f"Adding {file_path} to file group {dest_slug}.")
        endpoint.post_file_to_filegroup(path=file_path,file_type=file_type,metadata=file_metadata,file_group=file_group_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_slug", help="Filegroup source slug")
    parser.add_argument("--dest_slug", help="Filegroup destination slug")
    parser.add_argument("--request_id", help="Request ID")
    parser.add_argument("--kwargs", nargs="+", default=[], help="Optional key-value pairs to add as metadata (e.g., --kwargs key1=val1 key2=val2)")
    args = parser.parse_args()

    # Convert optional kwargs string list into a dictionary
    kwargs = {}
    for kwarg in args.kwargs:
        key, value = kwarg.split("=")
        kwargs[key] = value

    dupe_files(args.source_slug, args.dest_slug, args.request_id, **kwargs)
