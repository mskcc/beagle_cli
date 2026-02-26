## deregister_jobs_and_files.py

This will retrieve runs and files associated with a `request_id` and then builds a bash script that, when executed, will delete jobs through `beaglecli`

Prerequisite:

    The following env vars must be set:
        - BEAGLE_USER
        - BEAGLE_PW
        - BEAGLE_ENDPOINT

DEREGISTER_USER and DEREGISTER_PW are Beagle credentials allowed to delete.

Usage:

```
python3 deregister_jobs_and_files.py <request_id> <script name>
```

After the process, execute the script to complete the deregister.

For example:
```
python3 deregister_jobs_and_files.py 09603_I execute.sh
bash execute.sh
```

## dupe_metadata.py

The dupe_metadata.py script is a utility for duplicating files and their metadata from one filegroup slug to another within Beagle AI's endpoint. It takes source and destination filegroup slugs, a request ID, and optional key-value pairs as arguments. The script retrieves the file IDs for the given request and source slug, fetches the corresponding file metadata (including type and path), updates it with any provided additional metadata, and then copies it over to the destination filegroup slug.

This is useful when you need to replicate files and their associated data while adding or modifying specific metadata attributes in Beagle AI's environment.

Assumptions:

- Destination filegroup slug must exist.
- `BEAGLE_ENDPOINT`, `BEAGLE_USER`, and `BEAGLE_PW` env vars are set.

Usage:

Here's a usage example for `dupe_metadata.py`:

```
python scripts/dupe_metadata.py --source_slug lims --dest_slug destination-files --request_id 12345 --kwargs runId=ABCD_123 runMode="XSeq"
```

In this command:
- `lims` is the filegroup slug from which files are to be copied.
- `tmp-fg` is the filegroup slug to which files should be copied.
- `12345` is the ID of the request for which the files belong.
- `runId=ABCD_123 runMode="XSeq"` are optional key-value pairs that can be added as metadata when copying files.
