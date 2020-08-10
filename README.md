# beagle_cli
Beagle API Command Line Utility

#### Setup
- Requirements
  - `python 3`

- Instructions
  - `pip install -r requirements-cli.txt`

- Run
  - `./beaglecli`


Usage:
```
  beaglecli files create <file_path> <file_type> <file_group_id> [--metadata-path=<metadata_path>] [--size=<size>]

  beaglecli files list [--page-size=<page_size>] [--metadata=<metadata>]... [--file-group=<file_group>]... [--file-name=<file_name>]... [--filename-regex=<filename_regex>]

  beaglecli storage create <storage_name>
  
  beaglecli storage list

  beaglecli file-types create <file_type>

  beaglecli file-types list

  beaglecli --version
```
 Examples:
- List files by the igoId
  ```
  beaglecli files list --metadata igoId:07973_BO_6
  ```
- Create registration of file into Beagle database
  ```
  beaglecli files create /path/to/fastq/file fastq 12345 --metadata-path=metadata.json
  ```
