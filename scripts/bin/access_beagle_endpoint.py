import json
import re
import os
import requests


class AccessBeagleEndpoint:
    def __init__(self):
        username = os.environ['BEAGLE_USER']
        password = os.environ['BEAGLE_PW']
        BEAGLE_ENDPOINT = os.environ['BEAGLE_ENDPOINT']
        self.auth = requests.auth.HTTPBasicAuth(username, password)
        self.API = BEAGLE_ENDPOINT

    def run_url(self, url):
        """
        Runs the url, which should contain all the parameters we'd need
        """
        req = requests.get(url, auth=self.auth, verify=False)
        return req.json()

    def get_file_ids(self, request_id):
        url =  "%s/v0/fs/files/?page_size=1000&metadata=igoRequestId:%s" % (self.API, request_id)
        data = self.run_url(url)
        file_ids = list()
        for result in data['results']:
            file_ids.append(result['id'])
        return file_ids

    def get_file_id_by_path(self, path):
        url = "%s/v0/fs/files/?page_size=1000&path=%s" % (self.API, path)
        data = self.run_url(url)['results']
        if len(data) > 1:
            print("Error retrieving file_id by path; multiple entries found")
        if 'id' in data:
            return data['id']

    def get_files_by_metadata(self, key_val, file_group):
        url = f"{self.API}/v0/fs/files/?metadata={key_val}&file_group={file_group}&page_size=1000"
        data = self.run_url(url)['results']
        return data

    def patch_file_metadata(self, file_id, metadata):
        url = f"{self.API}/v0/fs/files/{file_id}/"
        response = self.patch_url(url, metadata)
        return response.json()

    def start_operator_run_pairs(self, body):
        url = f"{self.API}/v0/run/operator/pairs/"
        response = self.post_url(url, body)
        return response.json()

    def update_cmo_sample_names(self, current_sample_name, new_sample_name, file_group="b54d035d-f63c-4ea8-86fb-9dbc976bb7fe"):
        files = self.get_files_by_metadata(f"cmoSampleName:{current_sample_name}", file_group)
        for file in files:
            sample_class = file['metadata'].get("sampleClass", None)
            ci_tag = self.format_sample_name(new_sample_name, sample_class)
            file_id = file['id']
            body = {
                "metadata": {
                    "cmoSampleName": new_sample_name,
                    "ciTag": ci_tag
                }
            }
            print(f"Updating {current_sample_name} to {new_sample_name} with ciTag:{ci_tag}")
            self.patch_file_metadata(file_id, body)

    def format_sample_name(self, sample_name, specimen_type, ignore_sample_formatting=False):
        """
        Formats a given sample_name to legacy ROSLIN naming conventions, provided that
        it is in valid CMO Sample Name format (see sample_pattern regex value, below)

        Current format is to prepend sample name with "s_" and convert all hyphens to
        underscores

        If it does not meet sample_pattern requirements OR is not a specimen_type=="CellLine",
        return 'sampleMalFormed'

        ignore_sample_formatting is applied if we want to return a sample name regardless of
        formatting
        """
        sample_pattern = re.compile(r"^[^0-9].*$")

        if not ignore_sample_formatting:
            try:
                if "s_" in sample_name[:2]:
                    return sample_name
                elif (
                        bool(sample_pattern.match(sample_name)) or "cellline" in specimen_type.lower()
                ):  # cmoSampleName is formatted properly
                    sample_name = "s_" + sample_name.replace("-", "_")
                    return sample_name
                return sample_name
            except TypeError:
                print("sampleNameError: sampleName is Nonetype; returning 'sampleNameMalformed'.")
                return "sampleNameMalformed"
        else:
            return sample_name
