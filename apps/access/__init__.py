import os
import sys
from collections import defaultdict
from urllib.parse import urljoin
from pathlib import Path
import shutil
import requests

FLAG_TO_APPS = {
    "msi": ("access legacy MSI", "microsatellite_instability"),
    "cnv": ("access legacy CNV", "copy_number_variants"),
    "sv": ("access legacy SV", "structural_variants"),
    "snv": ("access legacy SNV", "small_variants"),
    "bams": ("access legacy", "bam_qc"),
    "nucleo": ("access nucleo", "bam_qc"),
}

def access_commands(arguments, config):
    print('Running ACCESS')

    request_id, sample_id, apps = get_arguments(arguments)
    tags = '{"cmoSampleIds":"%s"}' % sample_id if sample_id else '{"igoRequestId":"%s"}' % request_id
    if arguments.get('link'):
        for (app, app_version) in apps:
            (app_name, directory) = FLAG_TO_APPS[app]
            operator_run = get_operator_run(app_name, app_version, tags, config)
            if operator_run:
                if arguments.get('--single-dir'):
                    if app == "bams":
                        link_bams_to_single_dir(operator_run, app, request_id, sample_id, arguments, config)
                    else:
                        print("Apps other than bams not supported at this time")
                else:
                    link_app(operator_run, directory, request_id, sample_id, arguments, config)

    if arguments.get('link-patient'):
        for (app, app_version) in apps:
            (app_name, directory) = FLAG_TO_APPS[app]
            operator_run = get_operator_run(app_name, app_version, tags, config)
            if operator_run:
                if(app == "bams"):
                    link_bams_by_patient_id(operator_run, "bams", request_id, sample_id, arguments, config)
                else:
                    link_single_sample_workflows_by_patient_id(operator_run, directory, request_id, sample_id, arguments,
                                                           config)

def get_operator_run(app_name, app_version=None, tags=None, config=None):
    latest_operator_run = {
        "tags": tags,
        "status": "COMPLETED",
        "page_size": 1,
        "app_name": app_name
    }

    if app_version:
        latest_operator_run["app_version"] = app_version

    response = requests.get(urljoin(config['beagle_endpoint'], config['api']['operator-runs']),
                            headers={'Authorization': 'Bearer %s' % config['token']},
                            params=latest_operator_run)

    latest_runs = response.json()["results"]
    if not latest_runs:
        print("There are no completed operator runs for this request in the following app: %s:%s" %
              (str(app_name), str(app_version)), file=sys.stderr)
        return None

    return latest_runs[0]

def get_arguments(arguments):
    request_id = arguments.get('--request-id')
    sample_id = arguments.get('--sample-id')
    app_tags = arguments.get('--apps')
    if request_id:
        request_id = request_id[0]

    apps = [] # [(tag, version), ...]
    for app in app_tags:
        r = app.split(":")
        if len(r) > 1:
            apps.append((r[0], r[1]))
        else:
            apps.append((r[0], None))

    return request_id, sample_id, apps


def get_runs(operator_run_id, config):
    run_params = {
        "operator_run": operator_run_id,
        "page_size": 1000,
        "status": "COMPLETED"
    }

    response = requests.get(urljoin(config['beagle_endpoint'], config['api']['run']),
                            headers={'Authorization': 'Bearer %s' % config['token']}, params=run_params)

    return response.json()["results"]

def get_run_by_id(run_id, config):
    response = requests.get(urljoin(config['beagle_endpoint'], config['api']['run'] + run_id),
                            headers={'Authorization': 'Bearer %s' % config['token']})

    return response.json()


def get_files_by_run_id(run_id, config):
    response = requests.get(urljoin(config['beagle_endpoint'], config['api']['run'] + run_id),
                            headers={'Authorization': 'Bearer %s' % config['token']})

    return response.json()["outputs"]

def get_file_path(file):
    return file["location"][7:]

def link_app(operator_run, directory, request_id, sample_id, arguments, config):
    version = arguments.get("--dir-version") or operator_run["app_version"]
    should_delete = arguments.get("--delete") or False

    path = Path("./")
    path_without_version = path / ("Project_" + request_id) / directory
    path = path_without_version / version
    path.mkdir(parents=True, exist_ok=True, mode=0o755)

    runs = get_runs(operator_run["id"], config)
    if not runs:
        return

    files = [] # (sample_id, /path/to/file)
    for run_meta in runs:
        run = get_run_by_id(run_meta["id"], config)
        if should_delete:
            try:
                os.unlink(path / run["id"])
                print((path / run["id"]).absolute(), file=sys.stdout)
            except Exception as e:
                print("could not delete symlink: {} ".format(path / run["id"]), file=sys.stderr)
        else:
            try:
                os.symlink(run["output_directory"], path / run["id"])
                print((path / run["id"]).absolute(), file=sys.stdout)
            except Exception as e:
                print("could not create symlink from '{}' to '{}'".format(run["output_directory"], path / run["id"]), file=sys.stderr)

    try:
        os.unlink(path_without_version / "current")
    except:
        pass

    if not should_delete:
        os.symlink(path.absolute(), path_without_version / "current")
    return "Completed"


def link_single_sample_workflows_by_patient_id(operator_run, directory, request_id, sample_id, arguments, config):
    version = arguments.get("--dir-version") or operator_run["app_version"]
    should_delete = arguments.get("--delete") or False

    path = Path("./") / directory

    runs = get_runs(operator_run["id"], config)
    if not runs:
        return

    for run_meta in runs:
        run = get_run_by_id(run_meta["id"], config)
        sample_id = run["tags"]["cmoSampleIds"][0] if isinstance(run["tags"]["cmoSampleIds"], list) else run["tags"]["cmoSampleIds"]
        a, b, _ = sample_id.split("-", 2)
        patient_id = "-".join([a, b])

        sample_path = path / patient_id / sample_id
        sample_path.mkdir(parents=True, exist_ok=True, mode=0o755)
        sample_version_path = sample_path / version

        if should_delete:
            try:
                os.unlink(sample_version_path)
                print(sample_version_path.absolute(), file=sys.stdout)
            except Exception as e:
                print("could not delete symlink: {} ".format(sample_version_path), file=sys.stderr)
        else:
            try:
                os.symlink(run["output_directory"], sample_version_path)
                print(sample_version_path.absolute(), file=sys.stdout)
            except Exception as e:
                print("could not create symlink from '{}' to '{}'".format(sample_version_path.absolute(), run["output_directory"]), file=sys.stderr)

        try:
            os.unlink(sample_path / "current")
        except:
            pass

        if not should_delete:
            os.symlink(sample_version_path.absolute(), sample_path / "current")

    return "Completed"

def link_bams_to_single_dir(operator_run, directory, request_id, sample_id, arguments, config):
    version = arguments.get("--dir-version") or operator_run["app_version"]

    path = Path("./") / directory / ("Project_" + request_id)

    runs = get_runs(operator_run["id"], config)

    if not runs:
        return

    files = [] # (sample_id, /path/to/file)

    for run in runs:
        for file_group in get_files_by_run_id(run["id"], config):
            files = files + find_files_by_sample(file_group["value"], sample_id=sample_id)

    accepted_file_types = ['.bam', '.bai']
    for (sample_id, file) in files:
        file_path = get_file_path(file)
        _, file_ext = os.path.splitext(file_path)

        if file_ext not in accepted_file_types:
            continue

        file_name = os.path.basename(file_path)

        sample_id, _ = file_name.split("_", 1)
        a, b, _ = sample_id.split("-", 2)
        patient_id = "-".join([a, b])


        sample_path = path
        sample_version_path = sample_path / version
        sample_version_path.mkdir(parents=True, exist_ok=True, mode=0o755)

        try:
            os.symlink(file_path, sample_version_path / file_name)
            print((sample_version_path / file_name).absolute(), file=sys.stdout)
        except Exception as e:
            print("Could not create symlink from '{}' to '{}'".format(sample_version_path / file_name, file_path), file=sys.stderr)
            continue

        try:
            os.unlink(sample_path / "current")
        except Exception as e:
            pass

        try:
            os.symlink(sample_version_path.absolute(), sample_path / "current")
        except Exception as e:
            pass

    return "Completed"

def link_bams_by_patient_id(operator_run, directory, request_id, sample_id, arguments, config):
    version = arguments.get("--dir-version") or operator_run["app_version"]
    should_delete = arguments.get("--delete") or False

    path = Path("./") / directory

    runs = get_runs(operator_run["id"], config)

    if not runs:
        return

    files = [] # (sample_id, /path/to/file)

    for run in runs:
        for file_group in get_files_by_run_id(run["id"], config):
            files = files + find_files_by_sample(file_group["value"], sample_id=sample_id)

    accepted_file_types = ['.bam', '.bai']
    for (sample_id, file) in files:
        file_path = get_file_path(file)
        _, file_ext = os.path.splitext(file_path)

        if file_ext not in accepted_file_types:
            continue

        file_name = os.path.basename(file_path)

        sample_id, _ = file_name.split("_", 1)
        a, b, _ = sample_id.split("-", 2)
        patient_id = "-".join([a, b])


        sample_path = path / patient_id / sample_id
        sample_version_path = sample_path / version
        sample_version_path.mkdir(parents=True, exist_ok=True, mode=0o755)

        if should_delete:
            try:
                shutil.rmtree(sample_version_path)
                print(sample_version_path.absolute(), file=sys.stdout)
            except Exception as e:
                print("could not delete folder: {} ".format(sample_version_path), file=sys.stderr)
        else:
            try:
                os.symlink(file_path, sample_version_path / file_name)
                print((sample_version_path / file_name).absolute(), file=sys.stdout)
            except Exception as e:
                print("Could not create symlink from '{}' to '{}'".format(sample_version_path / file_name, file_path), file=sys.stderr)
                continue

        try:
            os.unlink(sample_path / "current")
        except Exception as e:
            pass

        if not should_delete:
            try:
                os.symlink(sample_version_path.absolute(), sample_path / "current")
            except Exception as e:
                pass

    return "Completed"

def find_files_by_sample(file_group, sample_id = None):
    def traverse(file_group):
        files = []
        if not file_group:
            return []
        if type(file_group) == list:
            if len(file_group) > 1:
                return traverse(file_group[0]) + traverse(file_group[1:])
            elif file_group:
                return traverse(file_group[0])
        elif "file" in file_group:
            try:
                file_sample_id = file_group["sampleId"]
                if "File" == file_group["file"]["class"] and (not sample_id or
                                                              file_sample_id ==
                                                              sample_id):
                    return [(file_sample_id, file_group["file"])] + [(file_sample_id,
                                                                      f) for f in file_group["file"]["secondaryFiles"]]
            except Exception as e:
                print(e, file=sys.stderr)
        elif "class" in file_group:
            if file_group["class"] == "Directory":
                return find_files_by_sample(file_group["listing"], sample_id=file_group["basename"])
            # TODO pull patient id here
            elif file_group["class"] == "File":
                secondary_files = [(sample_id, f) for f in file_group["secondaryFiles"]] if "secondaryFiles" in file_group else []
                return [(sample_id, file_group)] + secondary_files

        return []

    return traverse(file_group)

