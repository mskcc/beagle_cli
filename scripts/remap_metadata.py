import os
import sys
import bin.access_beagle_endpoint as beagle_api

BEAGLE = beagle_api.AccessBeagleEndpoint()


if __name__ == "__main__":
    FILE_PATH = sys.argv[1]
    with open(FILE_PATH, "r") as f:
        remap = f.readlines()
        # Skip header
        remap = remap[1:]
        for line in remap:
            line = line.rstrip()
            request_id, primary_id, lagacy_sample_label, current_sample_label = line.split("\t")
            BEAGLE.update_cmo_sample_names(current_sample_label, lagacy_sample_label)

