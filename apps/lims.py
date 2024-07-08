import json
import os
import uuid
import argparse
import re
import requests
from requests.auth import HTTPBasicAuth

LIMS_URL = "https://igolims.mskcc.org:8443/LimsRest/api"
LIMS_USER = os.environ.get('LIMS_USER', '')
LIMS_PASS = os.environ.get('LIMS_PASS', '')
LIMS_AUTH = HTTPBasicAuth(LIMS_USER, LIMS_PASS)

def lims_commands(arguments, config):
    print(LIMS_USER)
    if arguments.get('metadata'):
        process_requests(arguments.get('--request-id'))

def get_samples_from_request(request_id):
    response = requests.get("%s/getRequestSamples?request=%s" % (LIMS_URL, request_id), auth=LIMS_AUTH,
                            verify=False)
    results = response.json()
    if "error" in results:
        print(request_id, results, response.url)
        exit()
    return [sample["igoSampleId"] for sample in results["samples"]]

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_cmo_to_metadata(sample_ids):
    ret = []
    for sample_id_group in list(chunks(sample_ids, 10)):
        params = map(lambda sid: "igoSampleId=%s" % sid, sample_id_group)
        params = "&".join(params)
        response = requests.get("%s/getSampleManifest?%s" % (LIMS_URL, params),
                                auth=LIMS_AUTH, verify=False)
        results = response.json()
        for result in results:
            ret.append(result)
    return ret

def process_request(request_id):
    sample_ids = get_samples_from_request(request_id)
    request_cmo_ids = get_cmo_to_metadata(sample_ids)
    return request_cmo_ids

def process_requests(request_ids):
    ret = []
    for p in request_ids:
        ret = ret + process_request(p)

    print(json.dumps(ret))

