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
