import requests
import os
from collections import defaultdict
from urllib.parse import urljoin
from pathlib import Path

def access_commands(arguments, config):
    print('Running ACCESS')
    if arguments.get('link'):
        return run_access_folder_link_command(arguments, config)


def get_pipeline(name, config):
    response = requests.get(urljoin(config['beagle_endpoint'],
                                    "{}?name={}".format(config['api']['pipelines'], name)),
                            headers={'Authorization': 'Bearer %s' % config['token']})

    return response.json()["results"]

def get_group_id(tags, apps, config):
    latest_run_params = {
        "tags": tags,
        "status": "COMPLETED",
        "page_size": 1,
        "apps": apps
    }

    response = requests.get(urljoin(config['beagle_endpoint'], config['api']['run']),
                            headers={'Authorization': 'Bearer %s' % config['token']}, params=latest_run_params)

    latest_runs = response.json()["results"]
    if not latest_runs:
        print("There are no runs for this id")
        quit()

    return latest_runs[0]["job_group"]


def run_access_folder_link_command(arguments, config):
    request_id = arguments.get('--request-id')
    sample_id = arguments.get('--sample-id')
    request_id = request_id[0]

    try:
        path = Path("./{}/bam_qc".format(request_id))
    except FileExistsError:
        pass

    try:
        pipeline = get_pipeline("access legacy", config)[0]
    except Exception as e:
        print("Pipeline 'access legacy' does not exist")
        quit()

    tags = "cmoSampleIds:%s" % sample_id if sample_id else "requestId:%s" % request_id
    apps = [pipeline["id"]]

    group_id = get_group_id(tags, apps, config)

    run_params = {
        "tags": tags,
        "status": "COMPLETED",
        "page_size": 1000,
        "job_groups": [group_id],
        "apps": apps
    }

    response = requests.get(urljoin(config['beagle_endpoint'], config['api']['run']),
                            headers={'Authorization': 'Bearer %s' % config['token']}, params=run_params)

    runs = response.json()["results"]

    # sample_id -> /path/to/file
    files = []
    for run in runs:
        response = requests.get(urljoin(config['beagle_endpoint'], config['api']['run'] + run["id"]),
                                headers={'Authorization': 'Bearer %s' % config['token']})
        for file_group in response.json()["outputs"]:
            files = files + find_files_by_sample(file_group["value"])


    for (sample_id, file) in files:
        os.symlink(file, "./{}/bam_qc/{}/{}".format(request_id, sample_id, os.path.basename(file)))

    #response_json = json.dumps(files, indent=4)
    #return response_json

def find_files_by_sample(file_group):
    def traverse(file_group):
        files = []
        if type(file_group) == list:
            if len(file_group) > 1:
                return traverse(file_group[0]) + traverse(file_group[1:])
            else:
                return traverse(file_group[0])
        elif "file" in file_group:
            try:
                sample_id = file_group["sampleId"]
                if "File" == file_group["file"]["class"] and (not sample_id or
                                                              file_group["sampleId"] ==
                                                              sample_id):
                    return [(sample_id, file_group["file"]["location"][7:])] + [(sample_id,
                                                                                 f["location"][7:]) for f in file_group["file"]["secondaryFiles"]]
            except Exception as e:
                print("ERROR:")
                print(e)
                print(file_group)
        return []

    return traverse(file_group)

