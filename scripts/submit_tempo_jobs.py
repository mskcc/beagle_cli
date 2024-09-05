import sys
import bin.access_beagle_endpoint as beagle_api

BEAGLE = beagle_api.AccessBeagleEndpoint()

COHORT_ID = ""
JOB_GROUP_ID = ""
PIPELINE = "Tempo"
PIPELINE_VERSION = "feature/voyager"


def pairwise(iterable):
    a = iter(iterable)
    return zip(a, a)


def create_pairs(sample_list):
    total_runs = 0
    pair_json = []
    if len(sample_list) % 2 != 0:
        sample_list.append(None)
    for t, n in pairwise(sample_list):
        total_runs += 2
        pair_json.append({"tumor": t, "normal": n})
    print(total_runs)
    return pair_json


def create_body(sample_list):
    pair_json = create_pairs(sample_list)
    body = dict()
    body["pairs"] = pair_json
    body["name"] = COHORT_ID
    body["request_id"] = COHORT_ID
    body["pipelines"] = [PIPELINE]
    body["pipeline_versions"] = [PIPELINE_VERSION]
    body["job_group_id"] = JOB_GROUP_ID
    return body


if __name__ == "__main__":
    sample_file_path = sys.argv[1]
    with open(sample_file_path, 'r') as f:
        sample_list = f.readlines()
    sample_list = [sample.rstrip() for sample in sample_list]
    pairs = create_pairs(sample_list)
    for pair in pairs:
        BEAGLE.submit_run()

