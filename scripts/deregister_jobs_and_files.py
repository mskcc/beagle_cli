"""
Given some job id associated with a request ID,
retrieves child job ids and file ids
and deregisters all of them
"""
import sys
import os
import json
from pprint import pprint
import bin.access_beagle_endpoint as beagle_api

BEAGLE = beagle_api.AccessBeagleEndpoint()


if __name__ == "__main__":
    REQUEST_ID = sys.argv[1]
    OUTSCRIPT = sys.argv[2]

    print("Searching beagle db for igoRequestId %s" % REQUEST_ID)
    files_to_deregister = set(BEAGLE.get_file_ids(REQUEST_ID))
    files_to_deregister = list(files_to_deregister)
    num_files_to_deregister = len(files_to_deregister)

    print("Found %i files to deregister; run \n\n\tbash %s\n\nto complete processing."
            % (num_files_to_deregister,OUTSCRIPT))

    with open(OUTSCRIPT, 'w') as output_file:
        for i in files_to_deregister:
            if i:
                output_file.write("../beaglecli files delete --file-id=%s\n" % i)
