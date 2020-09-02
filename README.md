# beagle_cli
Beagle API Command Line Utility

#### Setup
- Requirements
  - `python 3`

- Instructions
  - `pip install -r requirements.txt`

- Run
  - `./beaglecli`


##### Optional

You can export `BEAGLE_USER` and `BEAGLE_PW` environment variables to override interactive login.

To access other endpoints, export the environment variable `BEAGLE_ENDPOINT`.


##### Usage
```
  beaglecli files create <file_path> <file_type> <file_group_id> [--metadata-path=<metadata_path>] [--size=<size>]
  beaglecli files update <file_id> [--file-path=<file_path>] [--file-type=<file_type>] [--file-group=<file_group_id>] [--metadata-path=<metadata_path>] [--size=<size>]
  beaglecli files list [--page-size=<page_size>] [--path=<path>]... [--metadata=<metadata>]... [--file-group=<file_group>]... [--file-name=<file_name>]... [--filename-regex=<filename_regex>]
  beaglecli files delete --file-id=<file_id>...
  beaglecli storage create <storage_name>
  beaglecli storage list
  beaglecli file-types create <file_type>
  beaglecli file-types list
  beaglecli file-group create <file_group_name> <storage>
  beaglecli file-group list [--page-size=<page_size>]
  beaglecli etl delete --job-id=<job_id>...
  beaglecli run list [--page-size=<page_size>] [--request-id=<request_id>]...
  beaglecli run get <run_id>
  beaglecli run submit-request --pipeline=<pipeline> --request-ids=<request_ids> [--job-group-id=<job_group_id>] [--for-each=<True or False>]
  beaglecli update-requests <request_id>...
  beaglecli tempo-mpgen
  beaglecli tempo-mpgen override --normals=<normal_samples> --tumors=<tumor_samples>
  beaglecli --version
```
 Examples:
- List files by the sampleId
  ```
  beaglecli files list --metadata=sampleId:07973_BO_6

  ```
- Create registration of file into Beagle database
  ```
  beaglecli files create /path/to/fastq/file fastq 12345 --metadata-path=metadata.json
  ```
- Submit a run request of two request Ids through a pipeline called "argos", using an existing job_group_id:
  ```
  beaglecli run submit-request --pipeline=argos --request-ids=ABCDE_1,ABCDE_2 --job-group-id=FGHIJK-LMNOP-QRSTUV-WXY --job-group-id=FGHIJK-LMNOP-QRSTUV-WXYZ
  ```

#### Troubleshooting

If you're having issues, try deleting ~/.beagle.conf file and logging back in.

For any other issues, please contact CMO Informatics (bolipatc@mskcc.org).
