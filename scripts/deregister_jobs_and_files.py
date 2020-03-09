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


def get_jobs(request_id):
    """
    From request_id, get all related run data
    """
    data = BEAGLE.get_etl_jobs_by_request(request_id)['results']

    ids_fetch_samples = set()

    for job in data:
        run_type = job['run']
        if run_type == 'beagle_etl.jobs.lims_etl_jobs.fetch_samples':
            ids_fetch_samples.add(job['id'])

    return ids_fetch_samples

def get_children_from_job(run_id):
    """
    Retrieves children from main job
    """
    data = BEAGLE.get_etl_job(run_id)
    return data['children']

def get_run_type(run_id):
    """
    We're assuming we get just one record here
    """
    data = BEAGLE.get_etl_job(run_id)
    return data['run']

def get_file_id_from_run_id(run_id):
    """
    Get the file id from the run id that imported it 
    """
    data = BEAGLE.get_etl_job(run_id)
    file_path = data['args']['filepath']
    return BEAGLE.get_file_id_by_path(file_path)

if __name__ == "__main__":
    REQUEST_ID = sys.argv[1]
    OUTSCRIPT = sys.argv[2]

    print("Retrieving root etl job IDs for %s" % REQUEST_ID)
    FETCH_SAMPLE_JOBS = get_jobs(REQUEST_ID)
    #print(FETCH_SAMPLE_JOBS)

    files_to_deregister = set(BEAGLE.get_file_ids(REQUEST_ID))
    runs_to_deregister = set()

    number_of_fetch_jobs = len(FETCH_SAMPLE_JOBS)
    print("Compiling child jobs from %i fetched jobs..." % number_of_fetch_jobs)

    for job_id in FETCH_SAMPLE_JOBS:
        child_jobs = get_children_from_job(job_id)
        for child_job in child_jobs:
            run_type = get_run_type(child_job)
            if run_type == "beagle_etl.jobs.lims_etl_jobs.create_pooled_normal":
                pooled_normal_file_id = get_file_id_from_run_id(child_job)
                files_to_deregister.add(pooled_normal_file_id)
            runs_to_deregister.add(child_job)

    runs_to_deregister = list(runs_to_deregister.union(set(FETCH_SAMPLE_JOBS)))
    files_to_deregister = list(files_to_deregister)
#    CHILD_JOBS = get_child_jobs(REQUEST_JOB_ID)
#    print(CHILD_JOBS)
    num_files_to_deregister = len(files_to_deregister)
    num_runs_to_deregister = len(runs_to_deregister)
    print("Got %i files and %i runs to deregister. See beaglecli command output to execute."
            % (num_files_to_deregister,num_runs_to_deregister))

    with open(OUTSCRIPT, 'w') as output_file:
        for i in runs_to_deregister:
            output_file.write("../beaglecli etl delete --job-id=%s\n" % i)

        for i in files_to_deregister:
            if i:
                output_file.write("../beaglecli files delete --file-id=%s\n" % i)
