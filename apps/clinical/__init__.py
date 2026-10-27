import os
import sys
from urllib.parse import urljoin
from pathlib import Path
import requests

FLAG_TO_APPS = {
    "stride": ("stride", "stride"),
}
SAMPLE_TAG_KEYS = ["sample_ids", "cmoSampleIds", "cmoSampleName", "cmoSampleId"]
DEFAULT_PAGE_SIZE = 100


def clinical_commands(arguments, config):
    print('Running Linking')
    apps, app_prefix, page_size, max_pages = get_arguments(arguments)
    if arguments.get('link-patient'):
        for (app, app_version) in apps:
            (app_name, directory) = FLAG_TO_APPS[app]
            if app_prefix:
                app_name = f'{app_prefix} {app_name}'
            operator_runs = get_operator_runs(app_name, app_version, page_size, max_pages, config)
            if operator_runs:
                link_single_sample_workflows_by_patient_id(operator_runs, directory, arguments, config)
    return "Completed"


def get_arguments(arguments):
    app_tags = arguments.get('--apps')
    app_prefix = arguments.get('--app-prefix') or ''
    page_size = arguments.get('--page-size')
    page_size = int(page_size) if page_size else DEFAULT_PAGE_SIZE
    if arguments.get('--all-pages'):
        max_pages = None
    else:
        max_pages = int(arguments.get('--pages') or 1)
    apps = []  # [(tag, version), ...]
    for app in app_tags:
        r = app.split(":")
        if r[0] not in FLAG_TO_APPS:
            raise ValueError(f"Unknown app '{r[0]}'. Valid apps: {', '.join(FLAG_TO_APPS)}")
        if len(r) > 1:
            apps.append((r[0], r[1]))
        else:
            apps.append((r[0], None))

    return apps, app_prefix, page_size, max_pages


def get_operator_runs(app_name, app_version, page_size, max_pages, config):
    """Return the most recent completed operator runs for an app (newest first),
    pulling up to max_pages pages from voyager (all pages if max_pages is None)."""
    headers = {'Authorization': 'Bearer %s' % config['token']}
    params = {
        "status": "COMPLETED",
        "app_name": app_name,
        "page_size": page_size,
    }
    if app_version:
        params["app_version"] = app_version

    response = requests.get(urljoin(config['beagle_endpoint'], config['api']['operator-runs']),
                            headers=headers, params=params)
    response.raise_for_status()
    operator_runs = response.json()["results"]
    total = response.json()["count"]
    next_page = response.json()["next"]
    pages = 1
    while next_page and (max_pages is None or pages < max_pages):
        response = requests.get(next_page, headers=headers)
        response.raise_for_status()
        operator_runs.extend(response.json()["results"])
        next_page = response.json()["next"]
        pages += 1

    if not operator_runs:
        print("There are no completed operator runs for the following app: %s:%s" %
              (str(app_name), str(app_version)), file=sys.stderr)
        return None
    print(f"Pulled {len(operator_runs)} of {total} completed operator runs for {app_name} "
          f"({pages} page(s) of {page_size})", file=sys.stderr)
    return operator_runs


def get_runs(operator_runs, config):
    response_rslts = []
    for oprn in operator_runs:
        run_params = {
            "operator_run": oprn['id'],
            "page_size": 1000,
            "status": "COMPLETED"
        }
        response = requests.get(urljoin(config['beagle_endpoint'], config['api']['run']),
                                headers={'Authorization': 'Bearer %s' % config['token']}, params=run_params)
        response.raise_for_status()
        response_rslts = response_rslts + response.json()["results"]

    return response_rslts


def get_run_by_id(run_id, config):
    response = requests.get(urljoin(config['beagle_endpoint'], config['api']['run'] + run_id),
                            headers={'Authorization': 'Bearer %s' % config['token']})
    response.raise_for_status()
    return response.json()


def get_sample_id(run):
    for sample_key in SAMPLE_TAG_KEYS:
        if sample_key in run["tags"]:
            sample_ids = run["tags"][sample_key]
            if not isinstance(sample_ids, list):
                return sample_ids
            if len(sample_ids) > 1:
                print(f'Run {run["id"]} has multiple samples {sample_ids}, skipping', file=sys.stderr)
                return None
            return sample_ids[0]
    raise LookupError(f'Run {run["id"]} is missing Sample Meta Data')


def link_single_sample_workflows_by_patient_id(operator_runs, directory, arguments, config):
    version = arguments.get("--dir-version")
    should_delete = arguments.get("--delete") or False
    path = Path("./") / directory
    # runs come back newest first, so the first run seen for a sample is its most recent
    seen_sample_ids = set()

    for run_meta in get_runs(operator_runs, config):
        run = get_run_by_id(run_meta["id"], config)
        sample_id = get_sample_id(run)
        if not sample_id:
            continue
        if sample_id in seen_sample_ids:
            print(f'Skipping older run {run["id"]} for sample {sample_id}', file=sys.stderr)
            continue
        seen_sample_ids.add(sample_id)

        a, b, _ = sample_id.split("-", 2)
        patient_id = "-".join([a, b])
        sample_path = path / patient_id / sample_id
        sample_version_path = sample_path / version
        current_path = sample_path / "current"

        if should_delete:
            if not os.path.islink(sample_version_path):
                print("could not delete symlink: {} ".format(sample_version_path), file=sys.stderr)
                continue
            os.unlink(sample_version_path)
            print(sample_version_path.absolute(), file=sys.stdout)
            if os.path.islink(current_path) and os.readlink(current_path) == str(sample_version_path.absolute()):
                os.unlink(current_path)
            if len(os.listdir(sample_path)) == 0:
                print("no remaining sample versions, deleting sample directory {} ".format(sample_path), file=sys.stderr)
                os.rmdir(sample_path)
                if len(os.listdir(sample_path.parent)) == 0:
                    os.rmdir(sample_path.parent)
            continue

        sample_path.mkdir(parents=True, exist_ok=True, mode=0o755)
        if os.path.islink(sample_version_path):
            if os.readlink(sample_version_path) == run["output_directory"]:
                print(sample_version_path.absolute(), file=sys.stdout)
            else:
                print(f"Updating symlink to most recent run for: {sample_version_path}", file=sys.stderr)
                os.remove(sample_version_path)
                os.symlink(run["output_directory"], sample_version_path)
                print(sample_version_path.absolute(), file=sys.stdout)
        elif os.path.exists(sample_version_path):
            print("could not create symlink from '{}' to '{}', path already exists".format(
                sample_version_path.absolute(), run["output_directory"]), file=sys.stderr)
            continue
        else:
            os.symlink(run["output_directory"], sample_version_path)
            print(sample_version_path.absolute(), file=sys.stdout)

        if os.path.islink(current_path):
            os.unlink(current_path)
        os.symlink(sample_version_path.absolute(), current_path)

    return "Completed"
