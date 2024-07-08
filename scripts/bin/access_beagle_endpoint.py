import requests
import sys, os
import json

class AccessBeagleEndpoint:
  def __init__(self):
    username = os.environ['DEREGISTER_USER']
    password = os.environ['DEREGISTER_PW']
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
    url =  "%s/v0/fs/files/?page_size=1000&path=%s" % (self.API, path)
    data = self.run_url(url)['results']
    if len(data) > 1:
        print("Error retrieving file_id by path; multiple entries found")
    if 'id' in data:
        return data['id']
