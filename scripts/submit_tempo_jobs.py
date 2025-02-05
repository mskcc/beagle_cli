import sys
from time import sleep
import bin.access_beagle_endpoint as beagle_api

BEAGLE = beagle_api.AccessBeagleEndpoint()

PAUSE = 20
SAMPLES_PER_RUN = 20
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


def create_body(cohort_id, sample_list, job_group_id=None):
    pair_json = create_pairs(sample_list)
    body = dict()
    body["pairs"] = pair_json
    body["name"] = cohort_id
    body["request_id"] = cohort_id
    body["pipelines"] = [PIPELINE]
    body["pipeline_versions"] = [PIPELINE_VERSION]
    if job_group_id:
        body["job_group_id"] = job_group_id
    return body


def split_into_batches(sample_list):
    result = []
    cnt = 0
    new_list = []
    for s in sample_list:
        if cnt < 20:
            new_list.append(s)
            cnt += 1
        else:
            cnt = 0
            result.append(new_list)
            new_list = []
            new_list.append(s)
            cnt += 1
    result.append(new_list)
    return result


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("python3 submit_tempo_jobs.py <file_path> <cohort_id> [<job_group_id>]")
        exit(1)
    sample_file_path = sys.argv[1]
    cohort_id = sys.argv[2]
    if len(sys.argv) > 3:
        job_group_id = sys.argv[3]
    with open(sample_file_path, 'r') as f:
        sample_list = f.readlines()
    sample_list = [sample.rstrip() for sample in sample_list]
    lists_to_submit = split_into_batches(sample_list)
    for idx, l in enumerate(lists_to_submit, start=1):
        print(f"Submitting {cohort_id}_{idx} for samples {','.join(l)}")
        BEAGLE.start_operator_run_pairs(create_body(f"{cohort_id}_{idx}", l))
        print(f"Remaining {len(lists_to_submit) - idx}")
        sleep(PAUSE)

