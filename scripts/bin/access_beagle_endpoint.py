# pyright: ignore[reportMissingImports]
import requests
from requests.auth import HTTPBasicAuth
import sys, os
import json


class AccessBeagleEndpoint:
    def __init__(self):
        username = os.environ["BEAGLE_USER"]
        password = os.environ["BEAGLE_PW"]
        BEAGLE_ENDPOINT = os.environ["BEAGLE_ENDPOINT"]
        self.auth = HTTPBasicAuth(username, password)
        self.API = BEAGLE_ENDPOINT

    def url_get(self, url):
        """
        Runs the url, which should contain all the parameters we'd need
        """
        req = requests.get(url, auth=self.auth, verify=False)
        return req.json()

    def get_file_ids(self, request_id, fg_slug="lims"):
        fg = self.get_file_group_id_by_slug(fg_slug)
        url = (
            "%s/v0/fs/files/?file_group=%s&page_size=1000&metadata=igoRequestId:%s"
            % (
                self.API,
                fg,
                request_id,
            )
        )
        data = self.url_get(url)
        file_ids = list()
        for result in data["results"]:
            file_ids.append(result["id"])
        return file_ids

    def get_file_id_by_path(self, path, fg_slug="lims"):
        fg = self.get_file_group_id_by_slug(fg_slug)
        url = "%s/v0/fs/files/?file_group=%s&page_size=1000&path=%s" % (
            self.API,
            fg,
            path,
        )
        data = self.url_get(url)["results"]
        if len(data) > 1:
            print("Error retrieving file_id by path; multiple entries found")
        if "id" in data:
            return data["id"]

    def get_storage_all(self):
        url = "%s/v0/fs/storage/" % self.API
        data = self.url_get(url)["results"]
        return data

    def get_storage_id_by_name(self, name):
        storage_all = self.get_storage_all()
        for i in storage_all:
            storage_name = i["name"]
            if storage_name == name:
                return i["id"]
        return None

    def get_file_group_id_by_slug(self, s):
        url = "%s/v0/fs/file-groups/%s" % (self.API, s)
        response = self.url_get(url)
        if response.get("detail") == "Not found.":
            pass
        else:
            id_value = response["id"]
            return id_value

    def put_url(self, url, payload):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        response = requests.put(
            url + "/", auth=self.auth, verify=False, json=payload, headers=headers
        )
        if not response.ok:
            raise RuntimeError(f"PUT failed: {response.status_code} {response.text}")

    def post_url(self, url, payload):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        try:
            response = requests.post(
                url, auth=self.auth, verify=False, json=payload, headers=headers
            )
            if not response.ok:
                raise RuntimeError(f"POST failed: {response.status_code} {response.text}")
        except RuntimeError as e:
            print(f"An error occurred during the POST request: {e}")

    def put_metadata_into_file(self, file_id, metadata):
        url = "%s/v0/fs/files/%s" % (self.API, file_id)
        payload = {"metadata": metadata}
        self.put_url(url, payload)

    def post_file_to_filegroup(self, path, file_type, metadata, file_group):
        url = "%s/v0/fs/files/" % (self.API)
        payload = {"path": path, "file_type": file_type, "metadata": metadata, "file_group": file_group}
        self.post_url(url, payload)

    def get_file_metadata(self, file_id):
        url = "%s/v0/fs/files/%s" % (self.API, file_id)
        data = self.url_get(url)
        return data
