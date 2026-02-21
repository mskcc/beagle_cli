import requests
import sys, os
import json


class AccessBeagleEndpoint:
    def __init__(self):
        username = os.environ["BEAGLE_USER"]
        password = os.environ["BEAGLE_PW"]
        BEAGLE_ENDPOINT = os.environ["BEAGLE_ENDPOINT"]
        self.auth = requests.auth.HTTPBasicAuth(username, password)
        self.API = BEAGLE_ENDPOINT

    def run_url(self, url):
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
        data = self.run_url(url)
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
        data = self.run_url(url)["results"]
        if len(data) > 1:
            print("Error retrieving file_id by path; multiple entries found")
        if "id" in data:
            return data["id"]

    def get_storage_all(self):
        url = "%s/v0/fs/storage/" % self.API
        data = self.run_url(url)["results"]
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
        response = self.run_url(url)
        if response.get("detail") == "Not found.":
            pass
        else:
            id_value = response["id"]
            return id_value
